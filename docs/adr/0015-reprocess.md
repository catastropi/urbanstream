# ADR-0015: Bronze→Silver 재처리와 Gold 범위 교체

- 상태: 채택 (2026-11-22). 두 결정((a) Silver 날짜 단위 덮어보기, (b) 재처리 경로의 Gold 범위 교체)은 설계안을 검토한 뒤 승인받았다.
- 바꾸는 결정: ADR-0009 §3(`backfill_from_raw` 의 "두 루트를 합침"), ADR-0009 ⑤ 와 ADR-0008 의 "팩트는 DO NOTHING(불변)". DEFERRED D-96·D-99 를 닫고 D-21 을 갱신한다.
- 관련 코드: `streaming/silver_pointer.py`, `streaming/silver_read.py`, `batch/silver_rebuild.py`, `batch/reprocess.py`, `batch/gold_load.py`(`mode="replace"`), `batch/stage_runner.py`(`kind`), `dags/reprocess.py`, `sql/015_reprocess_run.sql`. 절차: `docs/runbooks/reprocess.md`.

## 1. 맥락

지금까지 Silver 는 `spark-silver` 의 파일 sink 하나로만 만들어졌다. Silver 규칙(`streaming/silver.py`, `config/silver.yaml`)을 고쳐도 지난 날짜에 다시 적용할 방법이 없었고(D-99), Gold 팩트는 `ON CONFLICT DO NOTHING` 이라 Silver 를 다시 쌓아도 이미 들어간 행은 바뀌지 않았다(D-96).

제약은 세 가지였다.

- 스트리밍 sink 폴더는 `_spark_metadata` 로그를 기준으로 읽힌다. 로그에 없는 파일은 보이지 않고, 로그에 있는 파일을 지우면 배치는 오류, 스트리밍은 조용히 누락한다(D-21). 이 폴더를 배치가 고치면 안 된다.
- 수집과 스트리밍은 켜 둔 채로 재처리할 수 있어야 한다(STEP 3A 이후의 운영 전제).
- Spark 는 `gold-runner` 한 곳에서만, 한 번에 하나씩 돈다(STEP 3A, D-211·D-212).

## 2. 결정

| # | 결정 | 이유 |
|---|---|---|
| ① | **Silver 는 별도 루트 + 날짜 단위 덮어보기.** 재처리 결과를 `data/silver_rebuild/<표>/<버전>/` 에 일반 배치로 쓰고, `data/silver_rebuild/_active/<표>.json` 이 날짜마다 어느 버전을 볼지 정한다. 포인터가 없는 날짜는 스트리밍 루트(`data/silver`)를 본다. 한 날짜는 정확히 한쪽에서만 온다 | ADR-0009 의 "합치기"는 규칙이 행을 빼는 정정을 반영하지 못하고(스트리밍 쪽 옛 행이 남음), 값이 바뀌는 정정은 `fetched_at`·offset 이 같아 어느 쪽이 남을지 정해지지 않는다. 스트리밍 폴더를 직접 교체하는 방식은 D-21 과 정면으로 부딪힌다 |
| ② | **게시 = 포인터 파일 하나의 `os.replace`.** 빌드가 대조·축소 가드를 통과해 `_build.json` 의 `status` 가 `ok` 일 때만 게시할 수 있다 | 게시 전에 무엇이 실패해도 읽는 쪽은 예전 그대로를 본다. 되돌리기는 포인터를 이전 값으로 쓰는 것이라 비용이 없다 |
| ③ | **읽기 함수 하나(`read_silver`)를 `gold_stage`·`silver_verify` 가 쓴다.** 잡을 시작할 때 포인터를 한 번 읽어 스냅샷으로 쓰고, 그 지문과 내용을 Gold manifest(`silver_pointer_sha`, `silver_pointer`)에 남긴다 | 어떤 Gold 적재가 어떤 Silver 버전을 읽었는지 추적된다. 포인터가 없으면 예전과 똑같이 스트리밍 루트만 읽는다(기존 시험 그대로 통과) |
| ④ | **Bronze 는 기본 `listing`(파티션 폴더를 직접 나열)으로 읽는다.** `sink_log`(루트를 읽음, 스트리밍과 같은 시야)도 고를 수 있다 | D-255 복구로 `_spark_metadata` 를 옮긴 3개 표는 11-16 이전 Bronze 파일이 sink 로그에 없다(D-258). 로그로 읽으면 Silver 에는 있는 행이 재처리 결과에서 빠진다. Spark 4.1.3 은 하위 파티션 폴더를 직접 읽을 때 부모 `_spark_metadata` 를 쓰지 않는다(시험으로 확인, 4장) |
| ⑤ | **중복 제거는 배치용 결정적 버전**: 키마다 `(fetched_at, _kafka_partition, _kafka_offset)` 이 가장 앞선 한 행. 변환은 스트리밍과 같은 `transform()` 을 그대로 부른다 | 규칙은 한 곳에만 있다. `listing` 이 실패한 배치의 파일을 함께 읽어도 같은 Kafka 메시지라 내용이 같다. 같은 범위를 두 번 재처리하면 `silver_processed_at` 을 뺀 행이 같다 |
| ⑥ | **축소 가드.** 재처리 결과 키 수가 현재 Silver 보다 적으면 게시하지 않는다. 규칙 변경으로 줄어드는 것이 의도라면 `allow_shrink` 를 켜고 사유를 남긴다. Bronze 정상 키와 재처리 키가 다르면(유실·초과) 역시 게시하지 않는다 | 입력을 덜 읽은 재처리(예: `sink_log` 로 D-258 구간을 읽음)가 Silver 를 줄이는 사고를 막는다 |
| ⑦ | **날짜 제한: KST 그제 이전만.** 하루 단위로 처리하고 한 번에 최대 31일 | 스트리밍은 오늘·어제에 계속 행을 붙인다. 덮어본 날짜에 나중에 쓴 스트리밍 행은 가려지므로, 재처리가 읽은 Bronze 의 마지막 적재 시각을 남기고 `silver_verify` 가 "[재처리 뒤 늦은 행]" 줄로 알린다(실패로 만들지 않음) |
| ⑧ | **Gold 팩트는 재처리 경로에서만 범위 교체.** `load_all(mode="replace")` 가 교체 대상 표의 날짜 범위를 지우고 스테이징을 충돌 절 없이 넣는다. 전체가 한 트랜잭션이다. 평소의 `gold_intraday`·`gold_daily` 는 그대로 upsert(팩트 DO NOTHING) | `DO UPDATE` 로 바꾸면 값 정정은 되지만 사라진 행은 남는다. 범위 교체여야 "재처리 후 Silver = Gold"를 등호로 확인할 수 있다(`verify_counts(exact=...)`). 마트가 이미 같은 방식(날짜 범위 삭제 후 삽입)이다 |
| ⑨ | **범위 교체 가드**: manifest 범위가 교체 범위와 같아야 하고, 교체 대상 표의 스테이징이 0행인데 DB 에 행이 있으면 거부한다(`allow_empty` 없이는). 키가 범위 밖에 이미 있으면 PK 오류로 전체가 되돌아간다 | 빈 스테이징으로 하루치를 지우는 사고를 막는다 |
| ⑩ | **모든 Gold 적재는 `pg_advisory_xact_lock(hashtext('urbanstream.gold_load'))` 를 잡는다** | Airflow 풀 `gold_io` 에 더한 2차 방어. 손으로 돌린 적재와도 겹치지 않는다 |
| ⑪ | **실행은 `gold-runner` 요청에 `kind` 를 더해 처리한다**: `gold_stage`(기본값. `kind` 가 없는 예전 요청도 이것), `silver_rebuild`(Spark), `silver_publish`(Spark 없이 포인터만). 재처리 요청의 key 에는 `kind` 를 붙여 Gold 요청과 서로 건너뛰지 않게 한다 | Spark 가 계속 한 컨테이너에서 하나씩만 뜬다. 메모리 계산(드라이버 1g, 한도 1536m)이 바뀌지 않는다. DB 기록은 모두 Airflow 쪽이 한다(ADR-0008 ⑤ 그대로) |
| ⑫ | **마운트**: `gold-runner` 만 `data/silver_rebuild` 에 쓴다. Airflow·`spark-gold` 는 읽기 전용, `spark-silver`(스트리밍)는 이 폴더를 보지 않는다. `data/silver` 는 모두 계속 읽기 전용 | 정책 시험(`tests/unit/test_compose_policy.py`)으로 고정. Gold 를 만드는 곳이 포인터 폴더를 못 보면 스트리밍 Silver 만 읽게 되므로 세 곳 모두 마운트해야 한다 |
| ⑬ | **이력: 마이그레이션 015 `ops.reprocess_run`.** 한 행 = 하루 재처리 하나. 상태 `running → silver_built → silver_published → gold_loaded → ok`, 게시 전 실패는 `failed`, 되돌리면 `rolled_back`. 끝나지 않은 재처리끼리는 날짜가 겹치지 못한다(`EXCLUDE USING gist`) | Airflow 재시도는 같은 `request_key` 행을 이어 쓴다. 게시 뒤 실패한 행은 사람이 확인해야 하므로 자동으로 `failed` 로 바꾸지 않는다 |

### 재처리 뒤 다시 계산하는 마트와 하지 않는 마트

- 다시 계산(그 날짜 범위): `mart.bus_arrival_drift`, `mart.bus_section_time`, `mart.weather_traffic_hourly`(기준선이 아직 없으면 건너뜀). 마지막에 `mart.refresh_all()`.
- 다시 계산하지 않음: `mart.anomaly_event`(이미 보낸 알림의 기록), `mart.incident_impact`(해제 뒤 한 번 확정하는 기록), `mart.congestion_threshold`(주 1회 판 단위). 필요하면 각 DAG 를 따로 돌린다.

## 3. 검토 후 보류

- **Delta Lake·Iceberg 의 `replaceWhere`**: 이 문제의 표준 해법이지만 ADR-0007 에서 Delta 도입을 보류했고, Silver 파일 약 6.9만 개(D-258)를 옮기는 일이 이번 단계보다 크다.
- **raw(gzip) → Bronze 재생**: 이번 범위가 아니다. `silver_rebuild` 의 Bronze 읽기 함수가 그 결과를 입력으로 받을 수 있게만 두었다.
- **옛 버전 자동 정리**: `python -m batch.silver_rebuild prune` 을 수동으로 쓴다(포인터가 가리키지 않고 72시간이 지났고 표마다 최근 2개 밖인 버전만 지움). DAG 에 넣을지는 디스크 사용량을 본 뒤 정한다(D-270).

## 4. 근거 시험

- 전제(설계안 S8): Spark 4.1.3 에서 하위 파티션 폴더를 직접 읽으면 로그 밖 파일도 보이고, 로그에서 지운 파일로 오류가 나지 않는다. 루트를 읽으면 로그만 본다. `tests/spark/test_silver_rebuild.py::test_reading_partition_dirs_ignores_parent_sink_log`.
- 완료 판정(감사 보고서 E장 STEP 1):
  - 같은 범위 2회 재처리 시 Silver·Gold 행 수와 값 동일: `test_rebuild_equals_streaming_silver_and_two_rebuilds_are_identical`, `tests/spark/test_reprocess_e2e_pg.py::test_reprocess_twice_gives_identical_silver_and_gold_and_passes_reconcile_and_dq`.
  - 중간 실패 주입 시 기존 Silver 보존: 표 사이 예외(`test_failure_in_the_middle_keeps_existing_silver`), 쓰는 도중 프로세스 강제 종료(`test_killed_rebuild_process_leaves_no_publishable_version`), Gold 세 번째 표에서 예외(`test_gold_failure_rolls_back_whole_day_and_retry_resumes_after_publish`).
  - 재처리 후 Silver 대조·DQ 통과: 같은 e2e 시험과 `test_after_publish_silver_verify_passes_and_late_streaming_rows_are_reported`.
- 결정 근거: 규칙 변경으로 행이 빠지고 값이 바뀌는 정정이 포인터로만 반영되고 합치기 방식이면 옛 행이 남는 것(`test_rule_change_removes_rows_and_changes_values_only_through_the_pointer`), upsert 로는 사라질 행이 남는 것(`tests/integration/test_reprocess_pg.py::test_replace_deletes_only_the_range_and_removes_rows_missing_from_stage`).

## 5. 한계 (숨기지 않는다)

- 덮어본 날짜에 재처리 뒤 들어온 스트리밍 행은 가려진다. 알림은 `silver_verify` 의 정보 줄뿐이고, 반영하려면 같은 날짜를 다시 재처리해야 한다.
- `silver_verify` 는 Bronze 를 sink 로그로 읽는다. D-258 대상 표의 11-16 이전 날짜를 `listing` 으로 재처리하면 그 날짜는 Silver 가 Bronze(로그 기준)보다 많아 "초과"로 보인다. 일일 대조는 최근 1일만 보고 재처리는 그제 이전만 받으므로 평소에는 겹치지 않는다. 재처리 자체의 대조(Bronze 정상 키 = 재처리 키)는 같은 `listing` 입력으로 잡 안에서 한다.
- 재처리가 도는 동안 `gold_intraday` 는 풀 `gold_io` 에서 기다린다. 하루치는 표별로 41~241초였으므로(6절) 하루씩 돌리면 대기 한도(태스크 45분) 안이다. 여러 날짜를 한 번에 돌린 시간은 재지 않았다(STEP 2).
- 포인터 파일이 하나의 실패점이다. 게시 전 값은 포인터 안의 이력과 `ops.reprocess_run.previous_version` 두 곳에 남는다.

## 6. 적용 뒤 고친 것 (2026-11-23, Mac)

첫 실제 재처리(11-10, 다섯 표)가 실행기 한도 30분을 넘겨 실패했고, 그동안 다른 컨테이너까지 unhealthy 가 됐다(DEFERRED D-282). 결정 ①~⑬ 은 그대로이고, 실행 방식만 아래처럼 바꿨다.

| 무엇 | 처음 | 지금 | 이유 |
|---|---|---|---|
| Bronze 읽기 | 표마다 2번(파일 수를 세는 읽기 + 캐시하는 읽기) | 1번. 파일 이름을 열로 붙여 읽으면서 캐시한다 | Bronze 는 1분마다 생긴 작은 파일이라 한 번 읽는 비용이 크다 |
| 현재 Silver 읽기(대조) | 표마다 5번(행 수·키 수·추가·삭제·변경을 따로 셈) | 1번 읽고 캐시 | 같은 이유 |
| 재처리 Spark 의 CPU 우선순위 | 다른 컨테이너와 같음 | `nice -n 10` (`GOLD_RUNNER_REBUILD_NICE`, 0 이면 끔) | 급하지 않은 작업이다. 스트리밍·Kafka·healthcheck 가 CPU 를 먼저 얻게 한다 |
| 실행기 한도 | 모든 요청 1800초 | 재처리 빌드 3600초(`GOLD_RUNNER_REBUILD_TIMEOUT_SEC`), Gold 스테이징 1800초 그대로 | Airflow 쪽 대기(`run_rebuild` 3600초)와 맞춘다 |
| `one_day` 재시도 | 1번, 5분 뒤 | 0번. 원인을 본 뒤 태스크를 Clear 하면 끝난 단계는 건너뛰고 이어 간다 | 한도에 걸린 빌드를 곧바로 처음부터 다시 하면 같은 부하가 30분 더 이어진다 |

고친 뒤 같은 날(11-10)을 표별로 다시 돌린 Mac 실측: `silver_weather` 41초, `silver_traffic_speed` 46초, 나머지 세 표를 한 번에 돌려 241초. 다섯 표 모두 `added`·`extra`·`changed`·`missing`·`removed` 가 0 이었고(규칙을 바꾸지 않았으므로 기대한 값), 실행 뒤 컨테이너 12개가 모두 healthy 였다.

같은 날 Airflow(D-280)와 Kafka healthcheck(D-281)도 고쳤다. 재처리와 직접 관계는 없지만, CPU 4개를 같이 쓰는 Mac 에서는 재처리 부하가 이 두 곳의 약점을 드러냈다.

