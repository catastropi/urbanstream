# ADR-0010 — Step 8: 데이터 품질(DQ)·모니터링·알림·이상 탐지

날짜: 2026-11-05. 상태: 구현됨(샌드박스 검증 · Mac 확인 대기, `docs/DEFERRED.md` H장).
사용자 승인: 2026-11-05 02:07 ("다 동의"), 구현 범위 확정 02:16 ("필요하다 생각하는 건 다 하고 Step 8 만, 이전·이후 단계와 호환 완벽").
모든 숫자는 **[가정] 시작값**이다 — 7일 뒤 데이터로 보정한다(D-121).

## 맥락
Step 7 까지 파이프라인은 돌지만 "깨져도 아무도 모른다"가 남아 있었다. `urbanstream.notify.send` 는 로그만 남기는 자리표시였고, 수집 공백(D-09)은 표시가 없었으며, Mac 이 잠들면 Airflow 가 실패하고 알림이 오탐이 된다(D-104). Step 9(API)와 Step 10(대시보드)이 읽을 "파이프라인 건강" 자료도 없었다.

## 결정

| ID | 결정 | 이유 | 한계 / 되돌리기 |
|---|---|---|---|
| D8-1 | 세 덩어리로 나눠 구현·전달: 8a 알림+실시간 감시, 8b DQ, 8c 이상 탐지 | 각각 독립 마이그레이션(009/010/011)·DAG·시험. 중간에 멈춰도 앞 덩어리가 쓸모 있다 | — |
| D8-2 | 감시는 별도 컨테이너 `monitor`(profile `monitor`, Airflow 이미지 재사용, 256m) | 새 이미지 빌드 없음. DAG 로 하면 Airflow 가 죽을 때 같이 죽어 알릴 수 없다 | 감시자 자신의 죽음은 못 알림(D-126). `docker compose stop monitor` 로 끈다 |
| D8-3 | 알림 = 웹훅(Discord 기본, Slack 은 `ALERT_WEBHOOK_KIND`) + 모든 알림을 `ops.alert_event` 에 기록 | 계정 없이 URL 하나로 되고, 기록이 남아 대시보드·감사에 쓸 수 있다. `notify.send(level, title, body, *, dedup_key)` 시그니처·로그 형식은 그대로 | best-effort(D-123). `ALERTS_ENABLED=false` 로 전송만 끈다 |
| D8-4 | 알림 위생: error·critical 만 푸시, warning·info 는 07:30 아침 요약. 같은 `dedup_key` 는 쿨다운(critical 30분·error 2시간·warning 6시간) 동안 횟수만 센다. 시간당 푸시 10건 상한. 열린 warning 이 24시간 넘으면 error 로 승격. 정상 복귀 시 해소 알림 1건(푸시했던 것만) | 알림 피로를 막는다. 상한 때문에 못 보낸 것은 아침 요약과 다음 푸시에 "N건 억제"로 나온다 | 숫자는 `config/alerts.yaml` |
| D8-5 | 잠자기 규칙: 수집기가 깨어나며 `host_resume`(잠든 초)을 audit 에 남기고, 모니터가 DAG 실패(pending)를 증거와 대조해 분류한다. host_stall 과 겹침 → warning 으로 격하(날짜별 한 건으로 묶음) / 침묵(`collector_blackout`)과만 겹침 → 격하 없이 묶기만 / 증거 없음 → error 유지(fail-safe). 6시간 넘는 정지는 error 1건. 모든 소스가 600초 이상 조용하면 `collector_blackout` | 잠자기로 실패한 DAG 가 오탐 경보가 되지 않으면서, 진짜 실패는 숨지 않는다 | `monotonic` 이 잠자는 동안 멈춘다는 전제는 [추정](D-120). 틀리면 격하만 안 될 뿐 오탐/누락은 없다. 모니터가 꺼져 있으면 pending 이 600초 뒤 DQ DAG 에서 원래 수준(error)으로 나간다 |
| D8-6 | `ops.load_run` 에 `dag_id` 열을 **덧붙이고** `job` 은 `'gold_load'` 로 둔다 | HANDOFF 7-8 의 "job 에 DAG 이름"은 마트 선행 조건(`job='gold_load'` 필터)과 시험을 깨뜨린다 → 열 추가로 목적(어느 DAG 의 적재인가)만 달성 | 009 이전 행은 NULL. 백필은 `backfill_<날짜>`(D-128) |
| D8-7 | 건강 상태는 Postgres 표에 둔다(`ops.source_status`, `layer_status`, `source_health`, `data_gap`, `call_event`). Prometheus·Grafana 는 만들지 않는다 | Docker VM 메모리 여유가 작다(약 4.9/10.9 GiB). Grafana 는 나중에 이 표를 바로 읽으면 된다 | Step 10 에서 재검토(D-129) |
| D8-8 | 소스 신선도 = 마지막 **성공** 이후 시간 ÷ 기대 간격(interval ÷ 대상 수, 기상청은 4500초 고정). 2.5배 degraded, 5배 down. 활성 창 밖 idle, 자료 없음 unknown, 영구 실패(인증 등) 즉시 down, 실패율·예산 80% degraded. 레이어(Bronze/Silver) 정체는 최신 parquet mtime 으로 보되 **원천 소스가 ok 일 때만** 알린다 | 소스가 죽으면 레이어도 멈추므로 알림 폭주를 막는다. 빈 것을 정상으로 치지 않는다 | D-122(정확한 lag 아님) |
| D8-9 | DQ 엔진: 규칙은 `config/dq_rules.yaml`(린터가 검사), 결과는 `ops.dq_result`(규칙×대상×날짜, 재실행은 교체). 7일 **관찰(shadow)** 동안은 기록만. `block_mart` 는 "시각이 미래인 행" 규칙에만 허용 | 숫자를 모르는 상태에서 알림·차단을 켜면 오탐으로 신뢰를 잃는다. 마트를 멈추는 권한은 가장 확실한 결함에만 | D-131(게이트는 전날 DQ 를 본다). 규칙 파일에서 `mode: enforce`/`shadow_until` 로 조정 |
| D8-10 | 신뢰도 신호에 `expected_state`를 둔다: 기준선이 28일 전이면 provisional 이 정상(pass), 지난 뒤에도 provisional 이면 warn. 돌발 → 링크 연결률 ~1% 는 정상이라 값만 기록(info) | "아직 데이터가 부족한 것"과 "고장난 것"을 구분한다(D-112) | — |
| D8-11 | 이상 탐지: 링크×요일×KST 시각 기준선의 로버스트 z = (속도−p50)/max(IQR/1.349, 2km/h), \|z\|≥3.5. 기준선이 잠정(28일 미만)이거나 표본<20 이면 `candidate`(기록만). IQR=0 이면 `degenerate_baseline`(기록만). 기준선 칸이 없으면 행을 만들지 않고 건수만 센다. 새로 발견된 `anomaly` 만 알림(warning → 아침 요약) | 평균·표준편차는 이상값에 끌려간다. 기준선이 못 미더운 동안의 알림은 소음 | 지연 최대 1시간 20분(D-124), 4주 전엔 알림 없음(D-125) |
| D8-12 | 완료 기준 재해석: "수집·처리 공백이 알림으로 드러난다". 하류(Bronze/Silver/Gold) 공백은 다음 실행이 스스로 메우고, **수집기 쪽 공백은 복구 불가**지만 `ops.data_gap` 에 남고 DQ 가 설명한다 | 실시간 API 는 과거 값을 다시 주지 않는다(D-09) | `backfill_from_raw` 는 설계만(D-99) |
| D8-13 | 보관: call_event 30일, source_health 90일, alert_event·data_gap·dq_result·anomaly_event 180일(주간 `warehouse_maintenance`). 열린 알림 상태는 지우지 않는다. 새 표·뷰는 같은 마이그레이션에서 GRANT(DEFAULT PRIVILEGES 안 씀). `api_reader`·`bi_reader` 는 ops **뷰만**(원본 표 금지), 마트 이상 표는 마트 규칙대로 SELECT | 최소 권한 유지(D-102) | — |

## 구성
- 코드: `src/urbanstream/notify/`(정책·전송·저장·핵심), `src/urbanstream/monitor/`(설정·감사 파싱·Kafka 읽기·상태·공백·분류·레이어·루프), `src/urbanstream/dq/`(규칙·검사·엔진), `batch/dq_jobs.py`, `batch/anomaly.py`, `collector/runner.py`(`SleepWatcher`).
- SQL: `009_ops_monitor.sql`(8a), `010_dq.sql`(8b), `011_anomaly.sql`(8c).
- DAG(총 11개): 기존 8 + `dq_report_daily`(05:00) · `silver_reconcile_daily`(04:30, pool gold_io) · `anomaly_scan`(매시 :35).
- 설정: `config/alerts.yaml`, `config/monitor.yaml`, `config/dq_rules.yaml`.

## 이전·이후 단계와의 호환
- Step 7: `notify.send` 시그니처·로그 형식 유지. `on_failure` 는 pending 기록이 실패하면 `send` 로 대체. `job='gold_load'` 유지, `dag_id` 만 추가(없이 호출해도 동작). 마트 DAG 의 `check_upstream` 에 `require_dq_ok` 추가(관찰 기간엔 통과).
- Step 9(API): `ops.v_pipeline_health`, `v_source_status`, `v_layer_status`, `v_load_recent`, `v_alert_recent`(본문 없음), `v_data_gap_recent`, `v_mart_freshness`, `v_dq_latest`, `v_dq_summary`, `mart.v_anomaly_recent` 를 읽으면 된다.
- Step 10(대시보드): 위 뷰를 BI 가 읽는다. Prometheus 는 필요해지면 추가.
- Step 11(CI): 시험은 Postgres 서비스 컨테이너와 `GOLD_TEST_DSN` 으로 돈다. Airflow 설치가 있으면 DAG 파싱 시험도 돈다.

## 검증 상태
샌드박스: 단위·통합 시험(Postgres 16, 실제 역할 `SET ROLE`), 변이 시험(코드를 일부러 깨서 시험이 잡는지 확인), `docker compose config`, Airflow 3.3.0 DAG 파싱. **Mac 미확인**: 이미지 빌드·기동, 실제 Kafka 읽기, bind-mount mtime, 실제 웹훅, 잠자기(`monotonic`), `silver_verify` 서브프로세스.
