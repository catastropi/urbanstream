# Step 8 런북 — 알림 · 감시 · DQ · 이상 탐지, 그리고 복구 훈련

결정 근거: `docs/adr/0010-step8-dq-monitoring.md`. 미확인·남은 일: `docs/DEFERRED.md` H장(D-120~D-137).
프로젝트 루트에서 `conda activate urbanstream`, Docker Desktop 켠 상태. 명령은 한 줄씩 복사하세요.

**검증 상태.** 샌드박스에서 단위·통합·변이 시험, `docker compose config`, DAG 파싱까지 통과했습니다. **Mac 에서의 이미지 재빌드·monitor 기동·실제 웹훅·잠자기 동작은 아직 미확인**입니다. 아래 결과를 알려 주세요.

## 1. 웹훅 만들기 (👤 사용자 작업 — 비밀입니다)
Discord: 서버 설정 → 연동 → 웹후크 → 새 웹후크 → URL 복사. Slack: Incoming Webhooks 앱으로 URL 생성.
**URL 은 `.env` 에만 넣습니다. 채팅·코드·문서에 붙이지 마세요.** 아래 두 줄을 `.env` 맨 아래에 직접 추가합니다(에디터로 열기).

```zsh
open -e .env
```

추가할 줄: `ALERT_WEBHOOK_URL=<복사한 URL>` · Slack 이면 `ALERT_WEBHOOK_KIND=slack` (Discord 는 생략). 저장 후 닫습니다.

## 2. 새 파일 풀기 + 켜기
```zsh
unzip -o urbanstream_full.zip -d .
```
```zsh
scripts/stack.sh up-monitor
```
하는 일: Postgres/Kafka 확인 → `migrate.py`(009·010·011 적용) → `db_bootstrap.py` → Airflow 이미지 재빌드(confluent-kafka 추가, 몇 분) → `monitor` 컨테이너 기동. Airflow 도 쓰고 있다면 같은 이미지이므로 이어서 재시작합니다.
```zsh
scripts/stack.sh up-airflow
```
새 DAG 3개(`dq_report_daily`, `silver_reconcile_daily`, `anomaly_scan`)는 일시 중지로 생깁니다. 켭니다.
```zsh
docker exec urbanstream-airflow-scheduler airflow dags unpause dq_report_daily
```
```zsh
docker exec urbanstream-airflow-scheduler airflow dags unpause silver_reconcile_daily
```
```zsh
docker exec urbanstream-airflow-scheduler airflow dags unpause anomaly_scan
```

## 3. 수집기 재시작 (잠자기 감지 반영, D-81)
`docker compose --profile collector restart collector` (호스트에서 돌리면 Ctrl+C 후 다시 `python -m collector run`). 재시작하지 않으면 `host_resume` 을 내지 않아 잠자기 격하가 작동하지 않고 DAG 실패는 원래 수준(error)으로 옵니다(안전한 쪽).

## 4. 웹훅 점검
```zsh
scripts/recovery_drill.sh webhook-test
```
채널에 `[ERROR] 웹훅 시험 (UrbanStream)` 이 오고, 본문의 `password=` 뒤가 `***` 로 가려져야 합니다. URL 이 화면에 출력되면 안 됩니다.

## 5. 상태 보기
```zsh
scripts/recovery_drill.sh status
```
`ops.v_pipeline_health`(소스·레이어·모니터·적재·마트 상태)와 최근 알림 10건. 처음 몇 분은 `unknown`(자료 없음)이 정상입니다 — Kafka 의 audit 를 읽어 오는 중입니다.

## 6. 복구 훈련 (👤 사용자 작업 — 일부러 고장 냅니다)
각각 "고장 → 알림 도착 → 고침 → 해소 알림" 을 봅니다.
```zsh
scripts/recovery_drill.sh kafka-down
```
약 3분 안에 `[CRITICAL] 감시: Kafka 감사 토픽을 읽지 못합니다`. 되돌리기:
```zsh
scripts/recovery_drill.sh kafka-up
```
```zsh
scripts/recovery_drill.sh spark-down
```
약 15~20분 뒤 `…적재 정체`(원천 소스가 정상일 때만). 되돌리기:
```zsh
scripts/recovery_drill.sh spark-up
```
잘못된 키: 수집기를 먼저 멈춘 뒤
```zsh
scripts/recovery_drill.sh bad-key
```
1~2분 뒤 `소스 seoul_incident down`. 수집기를 다시 켜면 해소 알림. (`.env` 의 실제 키는 쓰이지도 바뀌지도 않습니다.)

## 7. 잠자기 훈련 (👤 직접)
1) `python scripts/sleep_probe.py` 를 켜 두고 덮개를 1~2분 닫았다 엽니다. '잠잔 시간' 이 닫은 시간과 비슷하면 D-120 의 전제가 맞습니다 — 결과를 알려 주세요.
2) 수집기·monitor·Airflow 를 켠 채 덮개를 20분 이상 닫았다 엽니다. 열고 약 2분 뒤 `status` 에서 `ops.data_gap` 에 `host_stall` 이 기록되고, 그 사이 실패한 DAG 알림이 `[WARN]`(아침 요약)으로 내려가 있어야 합니다. 확인:
```zsh
docker exec urbanstream-postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT kind, gap_start AT TIME ZONE '"'"'Asia/Seoul'"'"', duration_sec FROM ops.data_gap ORDER BY gap_id DESC LIMIT 5"'
```

## 8. 7일 뒤 (관찰 기간 종료, 2026-11-12)
```zsh
docker exec urbanstream-postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT rule_id, target, status, round(value::numeric,3) AS value, threshold FROM ops.v_dq_latest ORDER BY status DESC, rule_id"'
```
`fail`·`unknown` 이 어떤 규칙에서 나오는지 보고 `config/dq_rules.yaml` 의 숫자를 조정한 뒤 알려 주세요(D-121).

## 막혔을 때
| 증상 | 확인 |
|---|---|
| `up-monitor` 에서 이미지 빌드 실패 | 로그 끝부분을 알려 주세요(비밀 값은 지우고). pip constraints URL 접근 문제일 수 있습니다 |
| monitor 가 `unhealthy` | `docker logs urbanstream-monitor --tail 50` — DB 접속/Kafka 접속 오류가 보입니다 |
| 알림이 안 옴 | `status` 의 최근 알림 `status` 열: `dropped`=URL 없음/꺼짐, `failed`=전송 실패(이유는 `reason`), `digest`=아침 요약 대기(정상) |
| 모든 소스가 계속 `unknown` | monitor 로그에서 Kafka 읽기 오류, 또는 수집기가 새 코드로 재시작되지 않았는지(audit 는 이전에도 나왔습니다) |
