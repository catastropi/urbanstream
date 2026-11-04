# Step 7a 런북 — Airflow + Spark 배치 (gold_intraday · gold_daily · backfill)

결정 근거: `docs/adr/0009-step7-airflow.md`. 미확인·남은 일: `docs/DEFERRED.md` G장(D-90~D-103).
프로젝트 루트에서 `conda activate urbanstream`, Docker Desktop 켠 상태. 명령은 한 줄씩 복사하세요.

**검증 상태.** 샌드박스(Airflow 3.3.0 · Postgres 16 · pyspark 4.1.3 · Java 21)에서는 단위 테스트, DAG 파싱, `airflow dags test` 로 세 DAG 모두 실제 Spark·실제 적재까지 성공했습니다(세 번 반복해도 행 수 불변). **Mac 에서의 이미지 빌드·컨테이너 기동·예약 실행은 아직 미확인**입니다. 아래 7번 결과를 알려 주세요.

## 0. 먼저 — 지금 돌고 있는 것은 그대로 둡니다

수집기, `spark-bronze`, `spark-silver` 는 끄지 않습니다. Airflow 는 그 위에 얹습니다. 메모리: Silver 가 한도의 83% 라서(D-07) Airflow 를 켠 뒤 `stats` 를 꼭 봅니다.

## 1. `.env` 에 새 항목 채우기 (내 쪽 비밀 값은 자동 생성)

```zsh
scripts/stack.sh init-env
```

추가되는 키 이름만 출력됩니다(`AIRFLOW_DB_PASSWORD`, `GOLD_LOADER_PASSWORD`, `AIRFLOW_FERNET_KEY`, `AIRFLOW_JWT_SECRET`, `AIRFLOW_ADMIN_PASSWORD` 등). 기존 값은 바꾸지 않습니다.

## 2. 켜기 (첫 빌드는 몇 분 걸립니다)

```zsh
scripts/stack.sh up-airflow
```

이 명령이 하는 일: 폴더 생성 → Postgres/Kafka 확인 → `migrate.py` → `db_bootstrap.py`(역할 `airflow`·`gold_loader`, DB `airflow` 생성, 이미 있으면 비밀번호만 맞춤) → Airflow 이미지 빌드(JDK + pyspark) → 스케줄러가 healthy 가 되면 API 서버·DAG 프로세서 기동. 끝에 컨테이너 3개가 모두 `healthy` 면 성공입니다.

## 3. 화면 접속

브라우저에서 `http://127.0.0.1:8080` 을 엽니다. 아이디는 `admin`, 비밀번호는 `.env` 의 `AIRFLOW_ADMIN_PASSWORD` 입니다. 터미널에 비밀번호를 출력하지 않으려면 파일을 직접 열어 해당 줄만 복사하세요(채팅에 붙이지 마세요).

## 4. DAG 켜기 (만들어질 때는 일시 중지 상태입니다)

```zsh
docker exec urbanstream-airflow-scheduler airflow dags unpause gold_intraday
```

```zsh
docker exec urbanstream-airflow-scheduler airflow dags unpause gold_daily
```

`backfill` 은 수동 전용이라 켜지 않아도 됩니다(직접 실행만 가능).

## 5. 수동으로 한 번 실행해 보기

```zsh
docker exec urbanstream-airflow-scheduler airflow dags trigger gold_intraday
```

1~2분 뒤 상태:

```zsh
docker exec urbanstream-airflow-scheduler airflow dags list-runs gold_intraday
```

`state` 가 `success` 면 됩니다. 로그는 화면의 DAG → Runs → `run` 태스크 → Logs, 또는 `data/airflow/logs/` 폴더에 있습니다. 로그에서 `gold_stage 시작: <날짜> ~ <날짜>` 가 **오늘 날짜(KST)** 인지 확인하세요.

## 6. DB 에 들어갔는지 확인

```zsh
docker exec urbanstream-postgres psql -U urbanstream -d urbanstream -c "select run_id, status, started_at from ops.load_run order by run_id desc limit 5"
```

```zsh
docker exec urbanstream-postgres psql -U urbanstream -d urbanstream -c "select count(*) from fact.traffic_speed"
```

같은 DAG 를 두 번 실행해도 `count(*)` 가 늘지 않는 것이 정상입니다(이미 적재된 행이므로). 새 수집분만큼만 늘어납니다.

### 지난 날짜 다시 쌓기 (backfill)

```zsh
docker exec urbanstream-airflow-scheduler airflow dags trigger backfill --conf '{"date_from":"2026-11-03","date_to":"2026-11-04"}'
```

최대 31일, 하루씩 순서대로 처리하고 마지막에 마트를 한 번 갱신합니다. 이 DAG 는 **Silver 에 이미 있는 데이터**를 다시 쌓습니다(Bronze → Silver 재생성은 D-99 로 미룸).

## 7. 알려 주세요 (Mac 확인 항목)

1. `scripts/stack.sh up-airflow` 가 끝까지 갔는지, 어디서 멈췄는지(빌드 로그 마지막 20줄. 비밀번호가 나오지 않는 부분만).
2. `scripts/stack.sh ps` 결과(컨테이너 3개 상태).
3. 수동 실행의 `state`, 그리고 로그의 `gold_stage 시작` 줄과 `gold_stage 완료 (N초)` 줄.
4. `scripts/stack.sh stats` 를 **gold_intraday 가 도는 동안** 한 번, **끝난 뒤** 한 번(스케줄러 컨테이너 메모리와 Silver 가 한도의 몇 %인지).
5. 20분 뒤 예약 실행이 저절로 한 번 돌았는지(`list-runs` 에 `scheduled` 가 보이는지).

## 문제가 생기면

| 증상 | 원인 후보 | 할 일 |
|---|---|---|
| `AIRFLOW_FERNET_KEY 가 .env 에 없습니다` | init-env 안 함 | 1번 실행 |
| `gold_writer 역할이 없습니다` | 마이그레이션 전 | `python scripts/migrate.py` |
| 빌드에서 `apt-get` 실패 | 베이스 이미지가 Debian 계열이 아님(D-91) | 로그를 알려 주세요. Dockerfile 수정 |
| 스케줄러가 healthy 가 안 됨 | 마이그레이션이 오래 걸림 / Compose 구버전(D-92) | `docker compose --profile airflow logs airflow-scheduler` |
| `Permission denied` (logs, gold_stage) | bind mount 소유자(uid 50000) | `chmod -R a+rwX data/airflow data/gold_stage` 후 재시작 |
| 태스크가 `OOM`/종료 코드 137 | 스케줄러 한도 부족 | `.env` 에 `AIRFLOW_SCHEDULER_MEM_LIMIT=4g` 추가 후 `up-airflow` |
| 실행이 `queued` 에서 안 움직임 | 풀 `gold_io` 슬롯이 사용 중(다른 실행이 도는 중) | 정상. 앞 실행이 끝나길 기다림 |
| 화면 로그인 실패 | `AIRFLOW_ADMIN_PASSWORD` 가 계정 생성 뒤에 바뀜 | 계정은 첫 기동 때만 만들어짐. 비밀번호 재설정 명령은 **미확인** — 알려 주세요 |

## 끄기

```zsh
COMPOSE_PROFILES=core,airflow docker compose stop airflow-dag-processor airflow-apiserver airflow-scheduler
```

수집기·Bronze·Silver·Postgres·Kafka 는 계속 돕니다.
