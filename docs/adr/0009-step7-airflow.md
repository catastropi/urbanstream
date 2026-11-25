# ADR-0009: Step 7 — Airflow + Spark 배치 설계 결정

- 상태: 채택 (2026-11-04). 사용자 지시: "최대한 자동화·컨테이너 효율적으로, 범위는 네 범주에서 최대한 확장, 호환 문제없으면 승인" → "dag 컨테이너 수가 적지 않다고 판단되면 승인". 이 범위(7a 부터)를 승인으로 받아 진행.
- 관련: ADR-0008(Gold), `batch/jobs.py`, `dags/`, `docker/airflow/Dockerfile`, `docs/runbooks/step7a-airflow.md`, `docs/DEFERRED.md` G절.

## 1. 결정 요약

| # | 결정 | 이유 | 앞·뒤 단계 호환 |
|---|---|---|---|
| ① | **Airflow 3.3.0** (`apache/airflow:3.3.0-python3.12`, arm64 확인됨), **LocalExecutor**, 컨테이너 3개: `airflow-scheduler`, `airflow-apiserver`, `airflow-dag-processor`. compose profile `airflow`, 포트 127.0.0.1:8080 | 단일 머신·태스크 수가 적어 Celery/Redis/워커/트리거러는 메모리만 쓴다. 3개가 Airflow 3 의 필수 최소 구성(scheduler + API 서버 + DAG 프로세서) | `core` 만 켜면 안 뜬다(메모리). Step 8 의 알림·감독 DAG 도 같은 컨테이너에 추가 |
| ② | **Spark 실행 = Airflow 이미지에 JDK + `pyspark==4.1.3`, `gold_stage` 를 서브프로세스로** (`python -m batch.gold_stage`, 비밀 없는 화이트리스트 환경). docker.sock 마운트(A)는 거절 | A 는 호스트 경로·권한·보안(컨테이너 탈출) 문제. B 는 컨테이너 한 개로 끝난다. 호출 지점을 `batch/jobs.py` 한 곳에 모아 나중에 DockerOperator 로 바꿀 수 있다 | Step 6 코드 무변경. `GOLD_STAGE_ROOT` 환경 변수만으로 실행마다 스테이징 폴더 분리 |
| ③ | **메타데이터 DB = 같은 Postgres 안의 별도 DB `airflow` + 역할 `airflow`**. `scripts/db_bootstrap.py` 가 멱등 생성(CREATE DATABASE 는 `migrate.py` 의 파일별 트랜잭션에서 못 돌려 `sql/` 마이그레이션이 아님). 접속 권한은 `airflow` 역할만 | Postgres 컨테이너를 늘리지 않는다. 업무 스키마(dim/fact/mart/ops)와 섞이지 않는다 | — |
| ④ | **Airflow 는 `gold_loader`(LOGIN, `gold_writer` 상속)로 업무 DB 에 접속**. 관리자 비밀번호·`env_file` 은 Airflow 컨테이너에 주지 않는다 | 최소 권한. D-79(로그인 역할 비밀번호) 일부 해소 | Step 9 `api_reader` 는 별개 |
| ⑤ | **윈도우는 KST 날짜 기준, 수렴형**: `gold_intraday`(매시 20분, 오늘), `gold_daily`(01:00, 어제~오늘 + `mart.refresh_all()`), `catchup=False`, `max_active_runs=1` | Mac 이 자서 실행을 놓쳐도 다음 실행이 같은 윈도우를 다시 처리해 메운다. 팩트 DO NOTHING·날씨/에피소드/감사 DO UPDATE 라 반복해도 같은 결과 | `gold_stage --from/--to`(KST 포함 구간) 인터페이스 그대로 |
| ⑥ | **풀 `gold_io`(슬롯 1)**: 스테이징+적재는 한 번에 하나. 스케줄러 시작 때 `airflow pools set` | Spark JVM(힙 1g) 둘이 동시에 뜨면 스케줄러 컨테이너 한도 초과 | — |
| ⑦ | **실행마다 스테이징 폴더 분리** `/data/gold_stage/<dag>__<run_id>__try<N>`, 성공 시 삭제·실패 시 보존, 72시간 지난 것은 `gold_daily` 가 정리. `cleanup_stage` 는 base 밖·심볼릭 링크 탈출·`__` 없는 이름을 거부 | 동시 실행·재시도가 서로의 Parquet 을 덮어쓰지 않는다. 지우기 사고 방지 | `GOLD_STAGE_ROOT` 만 사용 |
| ⑧ | **검증 후 성공 처리**: manifest 날짜·생성 시각·`audit_available`·행 수 검사 → 적재 → DB 행 수 ≥ 스테이징 행 수 확인. `audit_available=false` 는 DAG 실패 | `absent_since` 가 전부 NULL 이 되는 조용한 오염 방지. 행 수 검증은 `row_counts`(스테이징 행)가 삽입 행이 아니라는 점을 보완 | Step 8 DQ 가 같은 manifest 를 읽을 수 있다 |
| ⑨ | **알림 인터페이스 고정**: `urbanstream.notify.send(level, title, body, dedup_key)`. 지금은 로그 + 비밀 마스킹 스텁, 모든 DAG 의 `on_failure_callback` 이 호출 | Step 8 이 시그니처 변경 없이 채널(슬랙 등)만 붙인다 | Step 8 |

## 2. 범위 분할

- **7a (이번)**: 인프라 + `gold_intraday`, `gold_daily`, `backfill`(수동, 하루씩 mapped 태스크, 최대 31일, 마지막에 마트 한 번 갱신).
- **7b**: `baseline_build`(문제 A), `bus_delay`(문제 C).
- **7c**: `incident_impact_finalize`(B), `weather_traffic_analysis`(D), `warehouse_maintenance`.
- 문제 B·C·D 마트는 **Spark 가 아니라 Postgres SQL(PostGIS)을 Airflow 가 실행**한다. 데이터가 작다(링크 4개, 노선 1개, 격자 1개). Spark 는 Silver→스테이징에만 쓴다. 트레이드오프: 규모가 커지면(링크 수천 개) SQL 마트는 느려질 수 있다 → 그때 Spark 로 옮긴다. 마트에는 `provisional` 플래그와 표본 수를 둔다.

## 3. 일부러 미룬 것 (DEFERRED G절)

> 2026-11-22 갱신: `backfill_from_raw` 와 팩트 범위 교체는 STEP 1 에서 구현했다. 두 루트를 "합치는" 방식 대신 날짜마다 한쪽만 읽는 포인터 방식을 택했다. 결정과 이유는 ADR-0015.

`backfill_from_raw`(Bronze→Silver 재생성, 출력은 별도 루트 `silver_backfill`, `gold_stage` 가 두 루트를 합침 — `_first_per_key` 가 중복 제거; 설계만), 보관 정리·Bronze 컴팩션(D-23/D-24; 위험: 전체 이력 에피소드 재계산 × 보관 삭제), `dq_report_daily`·`silver_reconcile_hourly`·`platform_supervisor`(Step 8), `reference_data_sync`(공휴일 D-74, 포털 승인 필요), 팩트 "범위 교체" 재계산 모드(팩트는 DO NOTHING).

## 4. 검증 상태 (샌드박스 = 사실, Mac = 미확인)

- 사실(샌드박스, Airflow 3.3.0 · Postgres 16/PostGIS · pyspark 4.1.3 · Java 21): 단위 테스트, DagBag 파싱, `airflow dags test` 로 `gold_intraday`·`gold_daily`·`backfill` 이 실제 Spark 서브프로세스·실제 적재까지 성공, 세 번 반복해도 행 수 불변(멱등), 스테이징 폴더 정리, 수동 실행의 `data_interval_end` 가 채워져 윈도우가 의도대로 계산됨. `db_bootstrap.py` 멱등·`gold_loader` 가 `airflow` DB 에 접속 불가.
- 샌드박스에서 발견·수정: DAG `start_date`(처음 11-05)가 첫 수동 실행 날짜(11-04)보다 늦으면 Airflow 가 태스크 인스턴스를 만들지 않고 실행이 **아무것도 안 하고 '성공'** 으로 끝난다 → `start_date=2026-11-01`(`catchup=False` 라 과거 실행은 생기지 않음), 정책 테스트로 고정.
- **미확인(Mac)**: 이미지 빌드(Debian 계열 가정, `default-jre-headless` 설치), FabAuthManager 포함 여부와 `users create`, Compose v2.10.2 의 `service_healthy` 연쇄, 스케줄러 컨테이너(Spark JVM 포함)의 실제 메모리, Docker VM 합계 여유, bind mount(uid 50000) 쓰기 권한, 실제 Silver 크기에서의 gold_stage 소요 시간, 예약 실행(`schedule`)과 KST 시간대 동작(샌드박스는 `dags test` 수동 실행만).
- 메모리 계산: 한도 합계 = Postgres 0.75 + Kafka 1 + Bronze 1.5 + Silver 1.5 + Airflow 3 + 1 + 0.75 ≈ 9.5 GiB (Docker VM 약 10.7 GiB). 한도는 상한이지 사용량이 아니다 — 실측 후 조정(D-90).

## 5. 7b 결정 (2026-11-04 승인)

| # | 결정 | 비고 |
|---|---|---|
| ①' | DAG 2개, 컨테이너 추가 없음: `bus_delay`(매일 02:00 KST, 창 어제~오늘), `baseline_build`(매주 일 02:30 KST) | `gold_daily`(01:00) 다음 |
| ②' | **A**: 기준선 MV 갱신은 `gold_daily` 에 둔다(중복 없음). `baseline_build` = **혼잡 단계 임계값 표** `mart.congestion_threshold`(링크 × 요일유형 × KST 시각의 속도 p10/p25/p50/p90, 판=version_date) | 임계값은 분포에서 나온다(D-78). 칸당 20건 미만이면 분위수를 비우고 `sufficient=false`, 28일 미만이면 `provisional`. `mart.v_traffic_level` 이 최신 관측에 단계(severe ≤p10 < heavy ≤p25 < slow ≤p50 < normal)를 붙이고 표본이 부족하면 NULL |
| ③' | **C**: 시간표가 없어 "시간표 대비 지연"은 만들지 않는다. (i) **도착 예측 드리프트** `mart.bus_arrival_drift` = 같은 버스·정류소의 예측 도착 시각(event_time + arrive_in_sec)이 접근 중 얼마나 밀렸나(마지막 − 처음). (ii) **구간 통과 시간** `mart.bus_section_time` = 구간 k 첫 관측 → k+1 첫 관측 | (i) 접근 = 같은 (노선, 정류소, 차량) 예측이 20분 안에 이어지는 묶음. `arrive_code=2`(출발대기) 제외. 접근 시작 날짜(KST) 기준으로 지우고 다시 넣는다(하루 전부터 읽어 자정 넘는 접근을 한 덩어리로). (ii) `sect_ord` 가 정확히 1 증가·30분 이내일 때만. ±90초(관측 주기) 오차 |
| ④' | 계산은 **Postgres SQL**(`batch/marts.py`), 날짜 범위 삭제 후 삽입(한 트랜잭션) | 마트는 파생물이라 교체해도 안전(팩트는 DO NOTHING). Spark 불필요(ADR §2 의 근거와 같음) |
| ⑤' | 마이그레이션 `sql/006_mart_7b.sql`: 표 3개 + 뷰 4개 + **같은 파일에서 GRANT**(`gold_writer` 쓰기, `api_reader`·`bi_reader` 읽기) | `gold_writer` 의 기존 권한은 dim/fact/ops 만이라 mart 쓰기는 006 에서 추가해야 했다 |
| ⑥' | 두 DAG 모두 시작 전에 **그 창을 덮는 성공한 gold_load 가 최근(36h / 주간은 72h) 있는지** 확인하고 없으면 실패(재시도) | `ops.load_run.job` 이 항상 'gold_load' 라 DAG 구분은 못 하고 날짜 범위·상태·시각으로 판단 |
| ⑦' | 모든 마트 표에 표본 수·`provisional`/`sufficient`·`computed_at` | Step 8 DQ 입력 |

**한계(미확인)**: 구간 통과 시간은 `sect_ord` 가 실제 구간 경계에서 바뀌는지 Mac 데이터로 확인 전(D-103). 공휴일 없음(D-74)이라 `day_type='holiday'` 는 만들어지지 않는다. 속도 0(`speed_zero`) 행도 분위수에 들어간다(기준선 MV 와 같은 규칙; 0 의 의미가 미확인이라 분리하지 않음). 하루 1.5일치 데이터라 현재 결과는 전부 잠정.

**검증(샌드박스, 사실)**: 통합 테스트 18개(값·분리·멱등·권한), 변이 시험 3건(출발대기 필터 삭제·`sect_ord+1` 완화·표본 부족 처리 삭제) 모두 잡힘, `airflow dags test` 로 `bus_delay`·`baseline_build` 성공 + 선행 조건이 없으면 의도한 오류로 실패 + 재실행 멱등. **Mac 미확인**: 실제 데이터에서의 값, 예약 실행.

## 6. 7c 결정 (2026-11-04 승인: "승인")

| # | 결정 | 이유 |
|---|---|---|
| ①'' | DAG 3개, 컨테이너 추가 없음(총 8 DAG): `incident_impact_finalize`(매일 03:00 KST), `weather_traffic_analysis`(매일 03:30 KST), `warehouse_maintenance`(일 04:00 KST). 앞의 둘은 SQL 만 쓰므로 `gold_io` 풀을 잡지 않는다 | Spark 를 쓰지 않는다(§2). 시각은 `bus_delay` 02:00 → `baseline_build` 일 02:30 다음 |
| ②'' | **B**: 해제(`absent_since`)된 지 24시간 지난 에피소드만 **한 번 확정**(`mart.incident_impact`), 확정된 행은 다시 계산하지 않는다. 장기 돌발(`is_long_term`)은 제외하고 개수만 센다 | 다시 나타나는 돌발을 거르고, 확정 이후 에피소드 원본 삭제를 가능하게 함(D-95) |
| ③'' | **B**: 연결은 ① 수집 링크와 `link_id` 일치 ② 아니면 수집 링크 300 m 이내(가장 가까운 것) ③ 없으면 `no_link` 행으로 남김. 영향 = 돌발 구간 속도 ÷ 같은 요일·시각 기준선 중앙값의 **중앙값**, 직전 60분과의 차이 `delta`. 구간 안 관측 3개 미만이면 `sufficient=false` | 수집 링크 4개라 연결률이 낮다 — 연결 건수를 같이 보여 주려고 `no_link` 도 저장 |
| ④'' | **D**: `mart.weather_traffic_hourly`(링크 × KST 시각) = 속도 중앙값 ÷ 같은 요일·시각 기준선 + 같은 시각 날씨(rn1·pty·t1h) + 강수 등급(none / light <1 mm / moderate / heavy ≥10 mm / snow_mix). 뷰 `v_weather_effect` 는 등급별 분위수, 시간 수 20 미만이면 `insufficient` | 요일·시각으로 나눠 출퇴근 효과를 걸러 낸다. 연관이지 인과가 아님. 날씨 격자는 1개 |
| ⑤'' | **창고 관리**: `ANALYZE`(소유자 권한 함수) → 표 크기·행 추정 기록(`ops.table_stats`) → `ops.load_run`·`ops.call_audit`·`table_stats` 180일 초과 삭제(실행 중 기록은 남김). **`VACUUM` 과 Bronze/Silver 파일 정리는 하지 않음** | `gold_writer` 는 소유자가 아님(D-110), Airflow 는 파일을 읽기 전용으로만 봄(D-21/D-113) |
| ⑥'' | 마이그레이션 `sql/007_mart_7c.sql` 에서 표·뷰·함수와 GRANT 를 같이. 두 마트 DAG 는 시작 전 `require_recent_load`(36h), 두 함수는 **기준선 MV 가 한 번도 안 채워졌으면 명확한 오류로 멈춤** | 샌드박스에서 비어 있는 MV 를 만나 DB 오류가 나는 것을 발견해 추가 |

**한계(미확인)**: Mac 실데이터 값(D-109), 돌발 연결률(D-112), 기준선 자기 포함(D-111).

**검증(샌드박스, 사실)**: 통합 테스트 6개(값·연결 방식·확정 규칙·불변·장기 제외·강수 등급·표본 부족·보관 삭제·권한·기준선 미충전), 변이 4건(확정 재계산 허용·비수집 링크 연결·장기 돌발 포함·눈 분류 제거) 모두 잡힘, 전체 274 passed / 2 skipped(Spark), `airflow dags test` 3개 성공(데이터는 비어 있어 배선만 확인).

## 변경 (2026-11-20, STEP 3A) — 결정 ② 의 Spark 실행 위치

**무엇이 바뀌었나.** Gold 스테이징의 Spark 는 이제 scheduler 컨테이너가 아니라 상주 컨테이너 `gold-runner`(spark-bronze 이미지)에서 돈다. Airflow 태스크는 `data/gold_stage/_requests/` 에 요청 파일을 남기고 결과 표시 파일(`.done`/`.failed`)을 기다린다. 규약은 `batch/stage_runner.py` 머리말.

**왜.** scheduler 메모리 사고가 세 번 있었다(D-211, D-212, D-257). 원인은 같다: Mac 절전으로 heartbeat 가 끊기면 실행 중인 Gold 작업이 고아로 판정되고, 두 번째 시도가 같은 컨테이너에서 Spark(작업 하나 약 1.6 GiB)를 또 띄운다. `max_active_runs=1`, 풀 `gold_io`, `execution_timeout` 은 이미 있었지만 같은 실행의 재시도 겹침은 막지 못했다. 실행기는 한 번에 요청 하나만 처리하고, 같은 실행의 재시도 요청이 같이 쌓이면 최신 것만 실행한다. 그래서 Spark 가 동시에 둘 뜨는 일이 구조적으로 없다.

**대안과 거절 이유.**
- docker.sock 을 Airflow 에 마운트하고 `spark-gold` 를 `docker run` 으로 띄운다: 처음 거절한 이유(컨테이너 탈출 위험)가 그대로다.
- 한도를 중복 실행분까지 올린다(3g → 5g 이상): Mac 메모리가 부족하고 원인(겹침)은 남는다.
- DockerOperator·KubernetesPodOperator: 소켓이나 클러스터가 필요하다.

**지킨 것.** `gold-runner` 에도 DB 접속 정보가 없다(ADR-0008 ⑤). Silver·Bronze 는 읽기 전용 마운트. 코드는 Airflow 와 같은 폴더를 읽기 전용으로 마운트해 두 컨테이너가 같은 코드를 쓴다.

**되돌리기.** `.env` 에 `GOLD_STAGE_MODE=inline` 을 넣고 Airflow 를 다시 만든다(`scripts/stack.sh up-airflow`). 그래서 Airflow 이미지의 JDK·pyspark 와 scheduler 메모리 한도는 그대로 두었다. inline 이 안정적으로 필요 없다고 확인되면(Mac 실측, D-212) 이미지에서 JDK 를 빼고 한도를 낮춘다.

**확인한 것.** 단위 시험(요청·중복 정리·취소·시간 초과·실행기 재시작), 실제 spark-submit 경로 시험(`tests/spark/test_stage_runner_spark.py`: 만든 폴더 권한이 777 이어서 Airflow 사용자가 지울 수 있음), 공식 Spark 4.1.3 이미지에서 spark 사용자로 실행해 스테이징 성공 후 uid 50000 으로 읽고 지우기. **Mac 에서 Airflow 와 함께 돌린 실측은 아직 없다.**

## 변경 (2026-11-22) — 실행기 박동·취소와 Gold 스테이징 읽기 범위 (D-274)

**무엇이 바뀌었나.**
- 실행기는 Spark 를 새 프로세스 그룹으로 띄우고, 기다리는 동안 5초마다 박동한다. 실행 중에도 `.cancel` 이 생기거나 시간 한도(기본 1800초)를 넘으면 그룹째 끈다(SIGTERM, 15초 뒤 SIGKILL). 1시간 넘게 줄 선 요청은 실행하지 않는다.
- Airflow 쪽 `request_stage` 는 같은 실행의 이전 시도가 남긴 요청이 아직 줄에 있거나 도는 중이면 새로 만들지 않고 그 결과를 기다린다. 줄 선 시간과 실행 시간을 따로 센다. 기다리는 동안 박동이 3분 넘게 멈추면 바로 실패한다.
- `gold_stage` 는 날짜 범위가 있으면 돌발 관측과 Bronze audit 도 파티션 날짜로 잘라 읽는다. 돌발 수명은 그 구간 값으로 만들고 적재 때 기존 행과 합친다.

**왜.** 11-22 Mac 에서 스테이징이 21분 → 30분 초과로 늘었고, 30분을 넘자 재시도가 '응답 없음'으로 즉시 실패했다. 박동이 요청 사이에만 갱신되는 설계는 '스테이징은 3분 안에 끝난다'는 가정에 기대고 있었고, 그 가정은 전체 이력 읽기 때문에 깨졌다. 둘 중 하나만 고치면 다른 쪽이 다시 문제를 만든다(STEP 1 재처리는 원래 몇 분 이상 걸린다).

**대안과 거절 이유.**
- 사팩트도 '최근 N시간'만: 사팩트는 이미 오늘 파티션만 읽어 이득이 작고, 늦게 온 행을 놓칠 여지가 생긴다.
- 수명을 DB 에서 SQL 로만 계산: Spark 와 SQL 에 같은 규칙이 둘 생긴다. 구간 값 + 합치기 규칙(LEAST/GREATEST, 관측 수 재계산)으로 충분하고, 하루씩 돌린 결과가 전체 이력 결과와 같음을 시험으로 고정했다.
- `RUNNER_STALE_SEC` 를 30분으로 늘리기: 실행기가 정말 죽은 경우를 30분 늦게 안다.

## 변경 (2026-11-23) — `gold_intraday`·`gold_daily` 를 증분으로 (D-278, D-279)

**무엇이 바뀌었나.** 두 DAG 는 날짜 윈도우를 통째로 다시 처리하지 않고, 진행 기록(`data/gold_stage/_state/gold_facts.json`) 이후 수정 시각이 새로운 Silver·audit 파일만 스테이징·적재한다(`jobs.process_incremental`). 밀리면 3시간씩 나눠 따라잡는다. 사라짐 시각은 적재 뒤 DB 가 다시 판정한다. Airflow 쪽 대기 한도는 잠자기를 세지 않는 시계로 잰다. `backfill` 은 그대로다.

**왜.** 결정 ① 의 '수렴형: 매번 같은 윈도우를 다시 처리' 는 하루치가 몇 분이면 맞는 선택이었다. 실제 Mac 에서는 하루치 사팩트를 다시 읽는 데 30분이 넘어 매시 실행이 한도에 걸렸다. 증분이면 실행 시간이 하루 중 시각과 무관해진다.

**대안과 거절 이유.**
- 사팩트를 `fetched_at` 으로만 거르기: 파일은 그대로 다 연다. 열기 비용이 큰 Mac 마운트에서는 효과가 작다.
- sink 로그(`_spark_metadata`)에서 새 배치 번호만 읽기: Spark 내부 형식에 기댄다. 수정 시각 기준은 형식과 무관하고, 로그 밖 파일 문제는 DO NOTHING·`ignoreCorruptFiles` 로 해결된다.
- 지난 날짜 압축(STEP 1 재처리 버전 활용): 오늘 실행에는 효과가 없다. 백필·검증이 느려지면 따로 한다.

**지킨 것.** 멱등은 그대로다: 진행 기록을 잃거나 지우면 전날 0시부터 다시 읽을 뿐 결과는 같다(DO NOTHING·합치기). 진행 기록은 적재·검증이 끝난 뒤에만 앞으로 간다.

**STEP 1 과 함께.** 재처리 포인터가 있는 날짜는 증분도 게시된 버전 폴더에서 새 파일을 고른다(`gold_stage.read_new_silver`, 한 날짜는 한쪽에서만). 게시 직후 그 버전 파일은 새 파일로 보여 한 번 다시 읽히지만 적재는 DO NOTHING 이다. 사라짐 판정(`EPISODE_ABSENT`)은 replace 적재에서도 같은 트랜잭션 끝에 돈다.
