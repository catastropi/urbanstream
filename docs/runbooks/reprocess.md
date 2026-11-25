# 재처리 (Bronze → Silver → Gold) 절차

지난 날짜의 Silver 를 고친 규칙으로 다시 만들고, Gold 팩트를 그 날짜만 지우고 다시 넣는다. 결정 근거는 `docs/adr/0015-reprocess.md`.

- 받는 날짜: KST 그제 이전, 한 번에 최대 31일, 하루씩 처리.
- 수집·스트리밍(`spark-bronze`, `spark-silver`)은 켜 둔 채로 해도 된다. 스트리밍 폴더(`data/silver`)는 건드리지 않는다.
- 평소 DAG(`gold_intraday`, `gold_daily`)는 그대로다. 재처리가 도는 동안 풀 `gold_io` 에서 차례를 기다린다.
- 필요한 것: STEP 1 적용(마이그레이션 015, `data/silver_rebuild` 폴더, `gold-runner` 재생성). `GOLD_STAGE_MODE=runner`(기본값).

## 1. 실행

Airflow UI(http://127.0.0.1:8080) → `reprocess` → Trigger DAG w/ config. 처음 한 번은 DAG 를 켠다(`airflow dags unpause reprocess`). 예약이 없는 수동 DAG 라 켜 두어도 저절로 돌지 않는다.

| 파라미터 | 뜻 | 기본 |
|---|---|---|
| `date_from`, `date_to` | KST 날짜, 양 끝 포함 | — |
| `tables` | `all` 또는 쉼표로 구분한 Silver 표 (`silver_traffic_speed`, `silver_bus_position`, `silver_bus_arrival`, `silver_incident_observation`, `silver_weather`) | `all` |
| `reason` | 왜 다시 만드는지. 비우면 실행되지 않는다 | — |
| `scope` | `silver`(Silver 만) / `silver_gold`(둘 다) / `gold`(지금 Silver 로 Gold 범위만 교체) | `silver_gold` |
| `allow_shrink` | 재처리 결과 키가 지금 Silver 보다 적어도 게시한다. 규칙이 행을 빼는 것이 의도일 때만 | `false` |
| `bronze_read_mode` | `listing`(파티션 폴더 직접 나열, sink 로그 밖 파일도 읽음) / `sink_log`(스트리밍과 같은 시야) | `listing` |

처음 해 볼 때는 하루, `scope=silver` 로 돌려 `row_counts` 의 `added`/`removed`/`changed` 를 본 뒤 `scope=gold` 로 같은 날을 이어 돌리면 안전하다.

Mac(CPU 4개)에서는 하루씩 돌린다. `silver_incident_observation` 은 그날까지의 Bronze 폴더를 모두 읽으므로 다른 표와 나눠 돌리는 편이 부하가 적다. 11-10 실측: `silver_weather` 41초, `silver_traffic_speed` 46초, 나머지 세 표 함께 241초(DEFERRED D-269).

명령으로 실행할 때:

```bash
docker compose exec airflow-scheduler airflow dags trigger reprocess \
  -c '{"date_from":"2026-11-10","date_to":"2026-11-10","tables":"all","reason":"silver.yaml 규칙 변경","scope":"silver"}'
```

## 2. 결과 확인

```bash
docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
  "SELECT reprocess_id, status, date_from, tables, silver_version, elapsed_sec FROM ops.v_reprocess_recent LIMIT 10"'
# 표별 숫자: 위 SELECT 에 row_counts 열을 더한다 (jsonb)
```

- `status=ok` 이면 끝. `row_counts` 의 표별 값: `bronze_rows`, `bronze_parse_ok`, `silver_before_keys`, `silver_after`, `added`, `removed`, `changed`, `missing`·`extra`(0 이어야 게시됨), `files_listed`·`files_with_rows`(차이 = 빈 파일 또는 건너뛴 깨진 파일).
- 지금 어느 날짜가 어느 버전을 보는지:

```bash
docker compose exec gold-runner python3 -m batch.silver_rebuild status
```

- 게시 뒤 늦게 들어온 스트리밍 행: `silver_reconcile_daily` 결과나 `scripts/silver_verify.sh` 출력의 `[재처리 뒤 늦은 행]` 줄. 나오면 같은 날짜를 다시 재처리한다.

## 3. 실패했을 때

| 상태 | 뜻 | 할 일 |
|---|---|---|
| `failed` | 게시 전에 멈췄다. Silver·Gold 모두 재처리 전 그대로 | `error` 열을 본다. `shrink_blocked` 면 의도인지 판단해 `allow_shrink=true` 로 다시, `reconcile_failed` 면 Bronze 를 확인. `3600초 안에 끝나지 않았다` 면 표를 나눠 다시 돌린다. 그 밖에는 원인을 고치고 다시 실행 |
| `silver_published` | Silver 는 새 버전, Gold 는 옛 값. `error` 에 사유 | Airflow 에서 같은 DAG 실행의 실패한 날 태스크를 Clear 하면 Gold 단계부터 이어 간다. 또는 4절로 Silver 를 되돌린다 |
| `gold_loaded` | Gold 까지 바뀌었고 마트 재계산에서 멈췄다 | 같은 태스크를 Clear. 마트만 다시 돌려도 된다(`bus_delay`, `weather_traffic_analysis`) |

하루 태스크(`one_day`)는 저절로 재시도하지 않는다. 원인을 본 뒤 Airflow 에서 그 태스크를 Clear 하면, 게시까지 끝난 단계는 건너뛰고 이어 간다.

한도: 재처리 빌드는 gold-runner 가 3600초(`GOLD_RUNNER_REBUILD_TIMEOUT_SEC`)에 끊는다. Gold 스테이징은 1800초 그대로다. 재처리 Spark 는 낮은 CPU 우선순위(`nice -n 10`, `GOLD_RUNNER_REBUILD_NICE`, 0 이면 끔)로 돈다. 돌고 있는 빌드를 멈추려면 요청 파일 옆에 취소 표시를 둔다. 5초 안에 꺼진다.

```bash
ls data/gold_stage/_requests/*rebuild*.running
touch data/gold_stage/_requests/<위 이름에서 .running 을 뺀 것>.cancel
```

`reprocess` 의 첫 태스크(`plan`)는 3시간 넘게 `running`·`silver_built` 에 멈춘 행을 `failed` 로 바꾼다. `silver_published`·`gold_loaded` 에 3시간 넘게 멈춘 행이 있으면 새 재처리를 시작하지 않고 멈춘다. 위 표대로 정리한 뒤 다시 실행한다.

## 4. 되돌리기

재처리 하나(`reprocess_id`)를 되돌린다. 버전 폴더는 남아 있으므로 파일을 복사할 일이 없다.

```bash
# 1) Silver 포인터를 게시 전 값으로 (포인터 이력에서 그 재처리의 게시 기록을 찾는다)
docker compose exec gold-runner python3 -m batch.silver_rebuild rollback --reprocess-id 17
#    그 뒤 다른 재처리가 같은 날짜를 다시 게시했다면 거부한다. 그래도 되돌리려면 --force
# 2) DB 기록
docker compose exec airflow-scheduler python -m batch.reprocess mark-rolled-back --id 17
# 3) Gold 를 되돌린 Silver 로 맞춘다: reprocess 를 같은 날짜·표, scope=gold 로 실행
```

`scope=silver` 로만 돌렸던 재처리는 1)과 2)만 하면 된다.

## 5. 옛 버전 정리

포인터가 가리키지 않고, 72시간이 지났고, 표마다 최근 2개 밖인 버전 폴더만 지운다.

```bash
docker compose exec gold-runner python3 -m batch.silver_rebuild prune --dry-run   # 지울 목록
docker compose exec gold-runner python3 -m batch.silver_rebuild prune
```

## 6. 수동 실행 (Airflow 없이, 확인용)

```bash
docker compose exec gold-runner sh -c 'SILVER_BRONZE_ROOT=/data/bronze SILVER_ROOT=/data/silver \
  nice -n 10 /opt/spark/bin/spark-submit --master local[2] --driver-memory 1g /app/batch/silver_rebuild.py build \
  --tables silver_weather --from 2026-11-10 --to 2026-11-10 --version m-test --report /data/gold_stage/m-test.json'
```

`build` 는 게시하지 않는다. 결과는 `data/silver_rebuild/<표>/m-test/_build.json`. 게시하려면 `publish` 를 같은 인자로 부른다(이 경우 DB 이력은 남지 않는다).
