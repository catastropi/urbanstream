# ADR-0006 — Step 5(Silver) 시작 전 결정: Silver 입력, Delta 보류, 과거 구간, 돌발 처리

- 상태: **승인** (2026-11-03 19:5x, 사용자 "승인"). Silver 표 설계(열, 워터마크 값)는 이 ADR의 범위가 아니다 — 별도로 제안·승인한다
- 관련: ADR-0005(Bronze), `streaming/bronze.py`, `docs/runbooks/step4-bronze.md`, HANDOFF 7-5
- 결정 방식: 사용자가 결정을 위임하고 승인했다. 아래는 *결정 + 이유 + 되돌리는 법 + 확인한 것/못 한 것*이다.

## 배경

Bronze 는 상시 가동 중이고(`bronze_verify` 통과), Step 5 를 시작하기 전에 HANDOFF 7-5 의 "Step 4 에서 넘어온 사항" 6개를 정해야 했다.
결정에 필요한 숫자는 사용자 Mac 에서 뽑았고(아래 실측 기록), 동작이 의심스러운 부분은 샌드박스(pyspark 4.1.3, Java 21, local)에서 실험했다.

## 결정 1 — Silver 는 Bronze 를 읽는다 (스트림). 입력 계약은 "Bronze 스키마"

- Silver 변환 함수의 입력은 `to_bronze_typed` 가 만드는 Bronze 스키마의 DataFrame 으로 고정한다. 읽는 곳은 `data/bronze/<표>` 의 스트리밍 파일 소스다.
- 이유: Kafka 보관이 72시간이라 Silver 로직을 고쳐 처음부터 다시 돌릴 수 있는 원천은 Bronze 뿐이다(원칙 '재처리 가능'). Bronze 는 이미 파싱된 열, `_parse_ok`, `_kafka_offset` 을 가져 행 수 대조가 쉽다. 수집 간격이 90~120초라 Bronze 를 거치는 추가 지연(트리거 60초)은 감수할 만하다(추정).
- Bronze 트리거는 **60초 유지**. 300초로 늘릴 이유가 실측으로 사라졌다(결정 2의 파일 수·로그 크기).
- 되돌리기: 입력을 Kafka 직접 읽기로 교체한다. `to_bronze_typed` 가 배치/스트리밍 공용이라 변환 코드는 그대로 쓴다.
- 대가: Bronze 가 멈추면 Silver 도 멈춘다. Bronze 는 `restart: on-failure:3`.

## 결정 2 — Delta 는 이번 Step 에서 도입하지 않는다. 대신 폴더 규칙을 둔다

- 규칙: **스트리밍이 쓰는 폴더는 배치가 건드리지 않는다. 배치가 쓰는 폴더는 스트리밍이 쓰지 않는다.**
- 이유(샌드박스 실험, Spark 4.1.3): 파일 sink 가 쓴 폴더는 `_spark_metadata` 로그가 읽기를 지배한다. 로그에 없는 파일을 같은 폴더에 두면 배치·스트리밍 읽기 모두 **보지 못한다**(행 20개 무시). 로그에 있는 파일을 지우면 배치 읽기는 `FILE_NOT_EXIST` 로 실패하고 스트리밍 읽기는 **오류 없이 그 행을 빼고** 읽는다(1,160 → 1,140).
- 귀결: ADR-0005 의 "일 단위 압축은 Step 7 배치" 를 **Bronze/Silver 폴더에 제자리로 하면 안 된다**. 재처리는 새 경로에 쓰고 교체하거나, 배치 전용 폴더에 쓴다.
- "지금 유효한 돌발" 같은 upsert 는 Gold(PostgreSQL `ON CONFLICT`)가 맡는다(설계 판단, 미검증).
- Delta 로 바꿀 신호: 스트리밍이 쓴 표를 제자리에서 압축해야 할 때 / Spark 안에서 MERGE 가 필요할 때 / `_spark_metadata` 의 가장 큰 compact 로그가 시작 시간을 눈에 띄게 늘릴 때. 전환 비용은 배치 재작성 1회.
- 확인한 것: Delta 4.1.0~4.4.1 은 Spark 4.1.0 용 `delta-spark_4.1_2.13` 아티팩트를 낸다(웹), Java 17+ 필요. 못 한 것: **4.1.3 패치에서 실제로 도는지**(샌드박스에서 Maven Central 접속 불가), 이미지에 jar 를 굽는 빌드.

## 결정 3 — `replay_raw.py` 는 만들지 않는다. `data/raw` 는 지우지 않는다

- 실측: `data/raw` 에는 `2026/11/03` 하루치(3.1 MB)뿐이다(11-02 폴더 없음). Kafka 에 없는 raw 전용 구간은 약 15:06~16:13 한 시간이다. 16:26~18:00 은 Kafka 에도 raw 에도 없다(수집기가 멈춰 있었다. 도로소통 raw 시간대별: 15시 26, 16시 13, 17시 0, 18시 30개).
- 이유: 한 시간을 채우려고 재생 경로를 만들 가치가 없고, 스트리밍 경로 재생은 워터마크 밖으로 도착해 중복이 새어 나온다(샌드박스: `dropDuplicatesWithinWatermark` 는 워터마크 5초에서 키 7종이 14행으로 나왔다 — 창이 지나면 같은 키를 다시 통과시킨다). 또 `rawstore` 는 본문과 파일명(KST 초 단위, `ingest_id` 앞 8자)만 남겨서 `http_status`·`latency_ms` 를 복원할 수 없다.
- 과거 구간이 정말 필요해지면 Step 7 의 `backfill_from_raw`(배치, 별도 경로)로 한다. 되돌리기: raw 만 보존하면 비용이 없다.
- 수집 공백(16:26~18:00)은 메우지 않고 **공백 구간으로 표시**한다(Silver/DQ 에서 audit·event_time 간격으로 탐지).

## 결정 4 — 형 변환은 `try_cast`

- 확인함(샌드박스 4.1.3): `spark.sql.ansi.enabled` 기본 true, `cast('abc' as int)` 는 오류, `try_cast` 는 NULL. 변환에 실패한 행은 버리지 않고 사유를 남긴다. 실제 컬럼의 실패 분포는 모른다.

## 결정 5 — Silver 는 별도 컨테이너(`spark-silver`), Bronze 와 같은 이미지

- 이유: 코드가 이미지에 구워져 있어(HANDOFF 3장) 같은 앱이면 Silver 를 고칠 때마다 Bronze 가 재시작된다. 쿼리 하나가 죽으면 프로세스 전체가 죽는 구조(`awaitAnyTermination`)다.
- 근거: Bronze 553~610 MiB(사용자 실측), Docker VM 7,851 MiB. 못 한 것: Silver 의 실제 메모리, dedup 상태 크기(수천 키 수준으로 추정). 되돌리기: 쿼리를 한 앱에 합친다.

## 결정 6 — Bronze→Silver 행 수 대조

- `_parse_ok=false` 행을 `_parse_error`(3종: `unparseable`, `schema_version_mismatch`, `missing_required`)별로 센다. 완료 기준의 "행 수 차이가 사유별로 설명됨"에 쓴다. 수집기 단계 reject 는 `dlq` 토픽으로 가므로 "수집 = 정상 토픽 + dlq" 로 본다. `dlq` 에 실제 메시지가 들어왔을 때의 동작은 아직 보지 못했다.

## 결정 7 — 돌발(seoul_incident) 처리

관찰: Bronze `seoul_incident` 에 `event_date=2026-04-07` 폴더가 있다. 최신 응답(`list_total_count` 7) 중 **2건이 2026-04-07 발생 `A04`(공사)** 이고 해제 예정이 2027-03-01, 2030-05-04 다(예: 탄천나들목 및 봉은교 구조개선공사, 양방향 전면통제). 파일 수가 도로소통의 정확히 2배(52 vs 26)인 것과 맞는다.

- **A. Bronze 는 바꾸지 않는다.** `event_date` 는 발생일 기준(ADR-0005 결정 2) 그대로. "Kafka 메시지 수 = Bronze 행 수" 와 `bronze_verify` 가 그대로 성립한다. 대가: 돌발은 오래된 날짜 폴더에도 계속 파일이 생긴다.
- **B. Silver 돌발은 워터마크 dedup 을 쓰지 않는다.** 활성 돌발은 2분마다 같은 키로 오고, 워터마크 dedup 은 창이 지나면 다시 통과시킨다. Silver 는 **관측 로그**(호출마다 append, `observed_at = fetched_at`)로 두고, 같은 호출 재전달 중복만 `(dedup_key, ingest_id)` 로 거른다. `first_seen`/`last_seen`/현재 유효 상태는 Gold 에서 만든다. 워터마크 기준 열은 `event_time`(발생 시각)이 아니라 `fetched_at`.
- **C. Silver 돌발 표는 `observed_date`(수집일, KST)로 분할**하고 발생일은 일반 열로 둔다. 쓰기가 항상 오늘 폴더로만 가고 보관·재처리가 날짜 단위로 단순해진다.
- **장기 공사는 제외하지 않는다.** 위 두 건은 전면통제라 도로 속도에 실제 영향이 있다. `is_long_term`(해제 예정 − 발생) 표시 열만 두고 기준 일수는 하루치 분포를 본 뒤 정한다(임의로 정하지 않는다).
- 되돌리기: Silver 는 Bronze 에서 다시 만들 수 있다 — 분할 열·dedup 방식 변경은 Silver 재생성 1회.
- 못 한 것: 같은 `acc_id` 의 내용 해시가 시간이 지나며 바뀌는 빈도(HANDOFF 는 20분 0/452), "목록에서 사라진 첫 관측 = 해제 시각" 여부.

## 실측 기록 (사용자 Mac, 2026-11-03 19:4x)

| 항목 | 값 |
|---|---|
| Parquet 파일 수 (Bronze 약 52분 라이브 관측) | audit 51, bus_arrival 102, bus_position 102, kma_ncst 2, seoul_incident 52, seoul_traffic 26 → 하루 약 8,300개로 외삽 (ADR-0005 의 상한 18,720 보다 2.3배 적다) |
| 표별 용량 (`du`) | audit 1.0M, bus_arrival 6.1M, bus_position 2.9M, kma_ncst 52K, seoul_incident 1.3M, seoul_traffic 636K → 합계 약 12 MB, 하루 약 0.15 GB 로 외삽 (`du` 반올림 포함) |
| `_spark_metadata` compact 로그 (bus_arrival) | `29.compact` 90줄 22,505 B → 엔트리 1건 약 253 B → 하루 약 2.1 MB, 30일 약 63 MB |
| `data/raw` | 3.1 MB, `2026/11/03` 하루치만 (bus_arrival 116, bus_position 117, kma_ncst 6, seoul_incident 90, seoul_traffic 91 파일) |
| Kafka 가장 오래된 도로소통 메시지 | CreateTime 1793690100000 = 2026-11-03 16:15 KST |
| 수집기 가동 공백 | 도로소통 raw 시간대별 15시 26, 16시 13, 17시 0, 18시 30, 19시 23 → 약 16:26~18:00 정지 |

## 다음

- 5-0 실행 완료(2026-11-04). 결과와 확정 규칙은 **ADR-0007**.
- Silver 표별 열, 좌표 변환 방식(pyproj UDF 기본), 기상 긴 형태 유지 — ADR-0007 에서 확정.
- 안 한 것·확인 못 한 것·어디에 추가할지는 모두 `docs/DEFERRED.md`.
