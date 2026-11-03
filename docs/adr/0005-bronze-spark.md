# ADR-0005 — Step 4 Raw/Bronze: Spark 버전·이미지, 저장 형식, 메모리 구성, 쓰기 방식

- 상태: **완료 기준 1·2 통과, 일부 후속 관찰 남음** (2026-11-03, 사용자 Mac 실측). 아래 "검증 상태"와 "실측 기록"에 무엇이 증명됐고 무엇이 아닌지 적었다
- 관련: `docker/spark-base/Dockerfile`, `docker-compose.yml`(spark-bronze), `streaming/`, `src/urbanstream/contracts/spark_schema.py`, `docs/runbooks/step4-bronze.md`
- 결정 방식: 사용자가 "지장 없다고 판단되면 진행"으로 위임했다. 아래는 *결정 + 이유 + 되돌리는 법*이다.

## 배경

Kafka 토픽 7개(`urban.*.v1` 5개, `urban.dlq.v1`, `platform.audit`)에 메시지가 쌓이고 있다(약 175건/분, 하루 20~25만 건). Bronze 는 이것을 **정제하지 않고 원래 모양 그대로** 파일로 남겨, 규칙이 바뀌어도 다시 처리할 수 있게 하는 층이다(원칙: 재처리 가능, lineage). 중복 제거·시간 보정·0값 해석은 Silver(Step 5)의 일이다.

## 결정 1 — Spark 4.1.x, Java 17, Scala 2.13, 공식 `apache/spark` 이미지

- 선택: `apache/spark:4.1.3-scala2.13-java17-python3-ubuntu` (태그 고정, `latest` 금지 — 테스트로 강제).
- 이유 (2026-11-03 웹 확인, endoflife.date): 4.0 은 지원이 2026-11-23 에 끝난다. 4.2.0 은 .0 릴리스(2026-07-11)라 ADR-0003 에서 Kafka 최신 .0 을 피한 것과 같은 기준으로 제외. 4.1 은 지원이 2027-06-11 까지이고 패치가 4.1.3 까지 나왔다. 3.5(LTS, 2027-11-30)는 안전하지만 Scala 2.12 계열이다.
- arm64: Docker Hub 에서 `apache/spark` 가 amd64·arm64 둘 다임을 확인했고, 사용자 Mac 에서 `docker manifest inspect apache/spark:4.1.3-scala2.13-java17-python3-ubuntu` 로 arm64 항목이 있음을 확인했다 (2026-11-03).
- Java 17: 4.x 가 17/21 을 지원하고, 공식 이미지의 기본 태그가 17 이다. 이 환경(Java 21)의 로컬 시험과 컨테이너(Java 17)는 JVM 이 다르다 → 컨테이너에서 `scripts/spark_test.sh` 를 한 번 돌려 같은 결과인지 확인한다.
- 4.x 주의: ANSI 모드가 엄격해 잘못된 캐스트가 NULL 이 아니라 오류가 된다. Bronze 는 캐스트를 하지 않지만(`from_json` 으로 읽기만), **Silver 에서는 `try_cast` 를 써야 한다**.
- 호스트 Python 과 Spark 는 무관해진다: Spark 는 컨테이너 안에서 돈다. 호스트 `urbanstream`(3.11) 환경에는 pyspark 가 필요 없다 (`tests/spark` 는 pyspark 가 없으면 건너뛴다).
- 되돌리기/올리기: `.env` 의 `SPARK_IMAGE`·`SPARK_VERSION` 두 값 (Dockerfile 의 `ARG` 기본값, compose 기본값, 태그가 서로 같아야 한다 — `test_compose_policy.py`). 3.5 로 내리면 Scala 가 2.12 로 바뀌어 `SCALA_BINARY` 도 같이 바꾼다 (테스트가 태그와 대조).

## 결정 2 — Bronze 는 Parquet append-only, `event_date`(KST) 한 단계 분할

- 이유: Bronze 는 추가만 하고 고치지 않는다(MERGE 불필요). 파일 sink 는 체크포인트와 `_spark_metadata` 로 재시작 때 중복 없이 이어 쓴다(결정 4). Gold 로 Postgres 에 `COPY` 할 때도 Parquet 이 단순하다.
- Delta 를 미룬 이유: Delta 4.x 가 Spark 4.0/4.1.0/4.2 와 짝지어져 있다고 문서에 나오지만 4.1.3 패치와의 짝은 확인하지 못했다. 안전한 조합은 "Spark 4.1 + Parquet" 또는 "Spark 3.5 + Delta 3.3" 이다.
- 약점: **작은 파일**. 트리거 60초면 하루 최대 약 18,700개(파티션 합 13 × 1,440), 300초면 약 3,700개(추정 — 실제 수는 `find data/bronze -name '*.parquet' | wc -l` 로 센다). 대응: 시작은 60초, 많으면 `BRONZE_TRIGGER_SEC=300`, 일 단위 압축은 Step 7 의 배치.
- Delta 로 바꿀 신호: 파일 수가 감당이 안 되거나, Silver 가 Bronze 를 스트림으로 읽을 때 파일 목록 조회가 병목일 때. Bronze 는 Kafka 와 `data/raw` 에서 다시 만들 수 있으므로 바꾸는 비용은 배치 재작성 1회다.
- 시간 단계 분할은 하지 않는다 (하루 20~25만 건에 시간 폴더는 작은 파일만 늘린다). `event_date` 는 **정상 행은 사건 시각(event_time)의 KST 날짜, 읽을 수 없는 행은 Kafka 메시지 시각의 KST 날짜**다 (둘 다 NULL 이 되지 않는다). 세션 시간대는 UTC 로 고정하고 날짜 계산에서만 KST 로 옮긴다 (ADR-0003 의 "저장은 UTC, 화면은 KST" 와 같은 원칙).

## 결정 3 — 컨테이너 1개, Spark 애플리케이션 1개, `local[2]`, 토픽별 쿼리 7개

- 설정: `--master local[2] --driver-memory 1g`, 컨테이너 한도 1536 MiB (`SPARK_BRONZE_MEM_LIMIT`), `spark.sql.shuffle.partitions=4`, Python UDF 없음(Python 워커 프로세스가 안 뜬다), Spark UI 는 127.0.0.1:4040 에만.
- 이유: Docker VM 7,851 MiB 에서 core 는 부하 중에도 **약 0.4 GiB**(kafka 340 MiB + postgres 22 MiB, 수집기 가동 중 실측 2026-11-03)다. 나머지 약 7.4 GiB 를 Spark·Airflow·API 가 나눠 써야 하므로 잡마다 컨테이너(JVM)를 따로 두지 않는다. 클러스터 모드(master/worker 컨테이너)는 JVM 만 늘어서 쓰지 않는다 — 확장 경로로 문서에만 남긴다.
- Bronze 는 중복 제거 같은 상태 연산이 없어 상태 저장소 메모리가 거의 들지 않는다. 메모리를 크게 쓰는 건 Step 5 의 dedup 상태다.
- 합계 추정(Step 5 이후 포함): core 0.4 + Bronze 1.5 + Silver 0.5~1.5 + Airflow 약 2 + API 0.3 = 약 4.7~5.7 GiB (**추정**). 한 쿼리가 실패하면 `awaitAnyTermination()` 이 예외를 올려 프로세스가 비정상 종료 → `restart: on-failure:3` 으로 재시작 → 체크포인트에서 이어간다 (3번 넘게 실패하면 멈춘다: 로그를 봐야 하는 상황).
- **실측 전까지는 추정이다.** 이미지를 올린 뒤 `scripts/stack.sh stats` 결과를 아래 "실측 기록"에 적는다. Mac 물리 메모리는 16 GiB 이므로 Docker VM 할당을 늘릴 여유가 있다 (Docker Desktop 설정).
- 되돌리기: 한도·힙은 `.env` 와 compose `command` 의 숫자 두 곳(테스트가 "한도 ≥ 힙 × 1.4"를 강제).

## 결정 4 — 파일 sink 를 쓰고 `foreachBatch` 를 쓰지 않는다. 읽을 수 없는 행도 버리지 않는다

- 이유: 파일 sink 는 체크포인트 + `_spark_metadata` 로 **정확히 한 번**에 가깝게 이어 쓴다. `foreachBatch` 는 배치가 재시도되면 같은 행이 두 번 쓰일 수 있어 직접 멱등 처리를 해야 한다.
- 그래서 "깨진 JSON 은 `_corrupt` 에 따로" 라는 HANDOFF 의 계획을 **같은 표 안의 표시 열**로 바꿨다 (분리하려면 `foreachBatch` 나 같은 토픽을 두 번 읽는 쿼리가 필요하다). `_parse_ok`(bool)·`_parse_error`(`unparseable` / `schema_version_mismatch` / `missing_required`)로 표시하고 `_raw_value` 에 원문이 남는다 → **Bronze 행 수 = Kafka 메시지 수**가 항상 성립한다 (완료 기준 1). 꺼내려면 `WHERE NOT _parse_ok`.
- 실측으로 확인한 점(`tests/spark`): JSON 이 아님/잘림/타입 불일치는 `from_json` 이 구조체를 NULL 이 아니라 "필드가 전부 NULL 인 구조체"로 돌려준다 → `columnNameOfCorruptRecord` 없이는 `{}` (필드 누락)와 구분이 안 된다.
- Kafka 읽기: `failOnDataLoss=true`(기본). 보관 기간(72시간)이 지나 읽기도 전에 지워진 구간이 있으면 조용히 건너뛰지 않고 쿼리가 실패한다 (유실을 숨기지 않는다). `maxOffsetsPerTrigger=50000`: 멈췄다 켜진 뒤 72시간 분량(60만 건 이상)을 한꺼번에 읽다 메모리가 터지는 것을 막는다 (값은 실측 후 조정).

## 결정 5 — 컬럼 구성과 `ingested_at`

- 정상 토픽 5개: envelope 11개 필드(최상위) + `payload`(구조체) + `_raw_value`(원문) + `_kafka_topic/partition/offset/timestamp/key` + `_parse_ok/_parse_error` + `event_date`.
- `dlq`, `audit`: 계약 스키마가 없어서 원문만 보존(`_raw_value` + `_kafka_*` + `ingested_at` + `event_date`). Step 8 에서 Silver 가 읽는다.
- `ingested_at`: 수집기가 비워 둔다(`null`) → Bronze 가 `current_timestamp()` 로 채운다. 이미 값이 있으면 보존. (재처리하면 다시 적재한 시각이 들어간다 — 의도된 의미: "Bronze 에 들어온 시각".)
- 스키마는 `src/urbanstream/contracts/spark_schema.py` 가 `schema_of()`(스냅샷과 같은 출처)에서 DDL 을 만든다. 새 계약 타입이 생기고 매핑이 없으면 조용히 틀리지 않고 `TypeError` 로 실패한다.
- 소스 토픽→표 이름: `config/sources.yaml` 의 소스 이름(`seoul_traffic` 등)을 그대로 폴더 이름으로 쓴다. 코드에 토픽 이름을 따로 적지 않고, `topics.yaml` 에 없는 토픽이면 시작할 때 실패한다.

## 결정 6 — Kafka 커넥터는 이미지에 넣는다 (`--packages` 금지)

- 실행할 때마다 내려받으면 시작이 느리고 네트워크가 필요하며, 같은 이미지가 날마다 다른 jar 로 돌 수 있다. 빌드 때 Spark 의 의존성 해석기(Ivy)로 한 번 풀고, 필요한 4개(`spark-sql-kafka`, `spark-token-provider-kafka`, `kafka-clients`, `commons-pool2`)만 `/opt/spark/jars` 에 복사한다 (Spark 가 이미 가진 lz4/snappy/zstd 등은 복사하지 않는다).
- 이미지는 비루트(`spark`, uid 185)로 실행하고, `.env`·`data/` 는 이미지에 들어가지 않는다 (`.dockerignore` + 테스트). 컨테이너에는 API 키를 전달하지 않는다 (Kafka 에 인증이 없다).

## 결정 7 — Spark 테스트는 운영과 같은 이미지 안에서 돌린다

- `scripts/spark_test.sh` 가 이미지 안에서 `tests/spark` 와 pyspark 가 필요한 `tests/unit/test_bronze_verify.py` 를 실행한다 → Mac 에 Java 를 깔 필요가 없고 Spark/Java/Python 이 운영과 같다. 호스트 `pytest` 는 pyspark 가 없으면 `tests/spark` 를 건너뛴다.
- 이 설계 환경에서는 pyspark 4.1.3 + Java 21 로 같은 테스트를 실제 Spark 로 돌렸다 (아래).

## 검증 상태 (정직하게)

| 확인한 것 | 방법 |
|---|---|
| 계약→Spark DDL, 설정, 토픽 커버리지, compose/Dockerfile 정책 | 호스트 테스트 (기존 150 + 신규 26). pyspark 가 있으면 **176 passed**, **pyspark 없는 깨끗한 환경(Mac 의 `urbanstream` 환경과 같은 조건)에서는 174 passed + 2 skipped** (`verify_bronze` 를 import 하는 테스트 2개가 건너뜀 — 컨테이너에서 실행) |
| 변환: 5개 소스 실제 샘플 왕복, KST 날짜, `ingested_at`, 읽을 수 없는 행 보존·표시, 스키마 버전 불일치 | **실제 Spark 4.1.3 (Java 21, local)** 로 `tests/spark` **46 passed** |
| 쓰기: `event_date` 분할 + `_spark_metadata`, 체크포인트 재시작 때 중복·유실 없음 | 같은 시험 (Kafka 대신 `rate` 소스를 Kafka 열 모양으로 바꿔 같은 쓰기 코드를 실행) |
| 테스트가 실제로 잡는지 | **변이 시험 6개 전부 잡힘**: KST→UTC 날짜, `ingested_at` 미채움, 깨진 JSON 판정 제거, 체크포인트 경로 매번 변경, 필수 필드 검사 제거, 스키마 버전 검사 제거 |
| **사용자 Mac 첫 실행 (2026-11-03 18:5x)** | 이미지 빌드 성공(529초, 커넥터 jar 단계 72초 통과), `spark-bronze` healthy, Java 17.0.19 / aarch64, 7개 표 폴더 생성. `bronze_verify.sh` 는 **검증 도구의 버그**로 실패: 토픽은 파티션 3개인데 키가 한 파티션에만 몰려 Bronze 에 그 파티션만 있었고, Spark 의 Kafka 배치 읽기는 파티션을 일부만 지정하면 거부한다 → Kafka 전체를 읽어 Spark 안에서 대조하도록 수정(`compare_with_kafka`), 그 상황을 재현하는 테스트 5개 추가 |
| **두 번째 검증 도구 버그** | 같은 날 재실행에서 `Unable to infer schema for Parquet`: 메시지가 아직 없는 토픽의 표 폴더는 `_spark_metadata` 만 있고 Parquet 이 없다. 빈 표를 `None` 으로 처리(Kafka 도 비었으면 OK, Kafka 에는 있는데 Bronze 가 비었으면 FAIL)하고, 표별 오류를 격리하도록 수정. 테스트 3개 추가, 변이 시험으로 확인 |
| **사용자 Mac 에서 확인됨 (2026-11-03 19:0x~19:2x)** | ① 이미지 빌드·커넥터 jar 4개 ② 실제 Kafka 소스(쓰기 잡 + `bronze_verify` 의 배치 읽기) ③ Java 17 컨테이너에서 `scripts/spark_test.sh` **57 passed, 실패 0**(jar 확인 테스트 포함) ④ `bronze_verify`: 5개 소스+audit **Bronze 행 수 = Kafka 메시지 수, 중복 0, 유실 0, 파싱불가 0**, dlq 는 토픽도 비어 있어 OK ⑤ `docker kill` 후 재시작(약 30초 내 healthy) → 재검증 **중복 0, 유실 0, 통과**, 표마다 행이 이어서 늘어남 |
| **아직 증명되지 않은 것** | ⓐ **배치 도중 kill**: kill 시점이 우연히 배치 안이었는지 알 수 없다(트리거 60초). 이 시험 1회는 "죽었다 살아도 중복·유실 없음"의 증거이지, 최악의 타이밍을 노린 증거는 아니다 — 여러 번 반복하면 확률이 높아진다 ⓑ **장시간 중단 후 따라잡기**(`maxOffsetsPerTrigger` 가 실제로 나눠 읽는지) — 중단은 약 30초였다 ⓒ 메모리의 장시간 추이(9분 시점만 측정) ⓓ 하루 단위 작은 파일 수 |

## 실측 기록 (사용자 Mac, 채워 넣을 것)

| 항목 | 값 |
|---|---|
| core 부하 중 (수집기 가동, 2026-11-03 18:04) | kafka 340 MiB, postgres 22 MiB |
| spark-bronze 재시작 후 약 1분 (2026-11-03 19:04, 수집기 가동) | **spark-bronze 553 MiB / 1.5 GiB (36%)**, kafka 359 MiB, postgres 22 MiB → 합계 약 934 MiB (VM 7,851 MiB 의 약 12%). 추정(1.5 GiB 근처)보다 훨씬 작음 |
| spark-bronze 재시작 후 약 9분 (19:17, 수집기 가동) | **spark-bronze 610 MiB / 1.5 GiB (40%)**, kafka 374 MiB, postgres 22 MiB → 합계 약 1.0 GiB. 첫 측정(553 MiB) 대비 +57 MiB — 추이는 장시간 관찰 필요 |
| 하루 Parquet 파일 수 (트리거 60초) | (미측정 — 하루 쌓인 뒤 `find data/bronze -name '*.parquet' \| wc -l`) |
| `bronze_verify.sh` 결과 (19:08, 19:1x kill 후) | 위 "사용자 Mac 에서 확인됨" ④⑤ — 두 번 모두 `전체: 통과` (seoul_traffic 40→44행, seoul_incident 673→704, bus_position 880→952, bus_arrival 9,567→10,340, kma_ncst 24, audit 240→255) |

## 남은 것

- `scripts/replay_raw.py`(`data/raw` → 토픽 재발행)는 만들지 않았다. Bronze 는 Kafka 가 새로 만들어진 시점(`earliest`)부터 쌓는다. 그 이전 구간(2026-11-03 15:06 이후 중 Kafka 재생성 전)은 `data/raw` 에만 있다 — 필요해지면(Silver 기준선 등) 다음 단계에서 정한다.
- **Kafka 를 지우면(`stack.sh reset` 등) Bronze 의 offset 이 새 Kafka 의 offset 과 겹친다** → `data/checkpoints/bronze` 와 `data/bronze` 를 같이 치워야 한다 (런북 7).

## 정정 1 (2026-11-03 저녁, ADR-0006 의 실측·실험 반영)

- **하루 파일 수:** 위 "작은 파일" 단락의 "하루 최대 약 18,700개(파티션 합 13 × 1,440)"는 상한이 과대했다. 소스 간격과 키 분포로 다시 계산하면 약 7,700개였고, 사용자 Mac 실측(Bronze 약 52분: audit 51, bus_arrival 102, bus_position 102, kma_ncst 2, seoul_incident 52, seoul_traffic 26 파일)을 외삽하면 **하루 약 8,300개**다(외삽이라 하루치로 다시 센다 — `docs/DEFERRED.md` D-23). 평균 파일 크기는 20~60 KB.
- **돌발 파일이 2배:** `seoul_incident` 가 `event_date` 폴더를 2개(`2026-04-07`, `2026-11-03`)로 나눠 쓰기 때문이다 (발생일 기준, 7개월 전 장기 공사가 응답에 계속 포함됨). Bronze 는 바꾸지 않는다 (ADR-0006 결정 7-A).
- **"일 단위 압축은 Step 7 배치"는 Bronze 폴더에 제자리로 하면 안 된다.** 파일 sink 폴더는 `_spark_metadata` 로그가 읽기를 지배한다: 로그에 없는 파일은 배치·스트리밍 읽기 모두 못 보고, 로그에 있는 파일을 지우면 배치는 오류, 스트리밍은 오류 없이 그 행을 빼고 읽는다 (샌드박스 Spark 4.1.3 실험, ADR-0006 결정 2). 압축은 새 경로에 쓰고 교체하는 방식이어야 한다.
- **"Silver 가 Bronze 를 스트림으로 읽을 때 파일 목록 조회가 병목"은 전제가 달랐다.** 스트리밍 읽기는 폴더를 훑지 않고 이 로그를 따른다. 대신 로그가 영구히 커진다: 엔트리 1건 약 253 B (Mac 실측 `29.compact` 90줄 22,505 B) → 하루 약 2.1 MB, 30일 약 63 MB (추정).
- **실측 기록 표 보강:** "하루 Parquet 파일 수" 칸 = 위 외삽값. 표별 용량 합계 약 12 MB(`du`, 약 117분치 데이터) → 하루 약 0.15 GB 로 외삽 (`du` 반올림 포함).
