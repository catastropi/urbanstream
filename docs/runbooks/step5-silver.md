# Step 5 런북 — Silver (진행 중)

결정 근거: `docs/adr/0006-step5-pre-decisions.md`. 안 한 것·확인 못 한 것·어디에 추가할지는 `docs/DEFERRED.md` (이 런북의 항목 번호 D-xx 는 그 장부의 번호다).
프로젝트 루트(`urbanstream/`)에서 `conda activate urbanstream` 상태로, Docker Desktop 과 수집기·Bronze 가 켜진 채로 실행합니다. 명령은 한 줄씩 복사하세요.

## 5-0. Bronze 프로파일링 (읽기 전용)

**무엇을 위한 것인가.** Silver 의 워터마크 값(D-02), 0값/결측 정책(D-03), 돌발의 장기 공사 기준(D-04)을 *가정이 아니라 데이터로* 정하려고 Bronze 를 읽기만 하며 숫자를 뽑습니다. 아무것도 쓰지 않습니다(결과 JSON 만 `data/profile/` 에 저장).

**1) 이미지 다시 만들기.** 코드가 이미지에 구워져 있어서 필요합니다. 돌고 있는 `spark-bronze` 는 재시작되지 않습니다.

```zsh
docker compose --profile core --profile streaming build spark-bronze
```

기대: 대부분 `CACHED` 이고 `COPY streaming` 단계만 새로 만들어져 수십 초 안에 끝납니다 (커넥터 jar 단계는 캐시, 529초가 다시 걸리지 않음 — 추정. 오래 걸리면 알려 주세요).

**2) 테스트 (선택, 권장).** 운영과 같은 이미지(Java 17)에서 프로파일링 테스트 21개가 돕니다. 설계 환경(Java 21, Spark 4.1.3)에서는 통과했습니다.

```zsh
scripts/spark_test.sh -k test_profile_bronze
```

기대: `21 passed`. (이미지에 테스트 코드는 넣지 않고 마운트해서 돌립니다.)

**3) 프로파일 실행.** 별도 JVM 이 하나 더 떠서 잠시 메모리를 더 씁니다(약 1 GiB 추정). 몇 분 걸릴 수 있습니다.

```zsh
scripts/silver_profile.sh 2>&1 | grep -v -E "INFO|WARN|^\s+at |Suppressed"
```

출력 전체를 붙여넣어 주세요. 표(소스)마다 아래 항목이 나옵니다.

| 항목 | 읽는 법 | 어디에 쓰이나 |
|---|---|---|
| `coverage` | 행 수, 파싱 실패, 수집 기간(KST) | 표본이 충분한지 |
| `hourly` | KST 시간대별 행 수 | 빈 시간대 = 수집 공백 (16:26~18:00 확인용) |
| `lateness_sec` | 먼저 호출된 행들 중 가장 늦은 event_time 보다 얼마나 뒤처졌나. `p99`, `max` 가 핵심 | **워터마크 지연 값의 근거** (D-02). 돌발은 의미 없어 나오지 않음 |
| `source_age_sec` | `fetched_at - event_time`. 버스위치면 GPS 나이 | 위 값의 구성 요소 확인 |
| `pipeline_delay` | `ingested_at - fetched_at` 구간별 건수 | 꼬리(`gt_10min`)는 Bronze 백필/재시작 구간이 대부분 — 워터마크 근거로 쓰지 않음 |
| `duplicates` | `same_call_dups` = 한 호출 안 중복(0 이어야 정상), `cross_call_dups` = 같은 사건을 여러 호출에서 봄 | Silver dedup 이 없애는 양 |
| `time_anomalies` | 미래 event_time, event > ingested, 시계 어긋남, 하루 넘게 전 | 완료 기준 "시간 역전/미래 0" 의 기준선 |
| `source_specific` | 소스별: 도로소통 `speed_zero`, 버스위치 `regress_pairs`/`same_event_time_pairs`, 버스도착 `arrive_in_sec_zero`+`arrival_msg_when_zero_top`, 돌발 `planned_duration`/`incidents_with_changed_content`, 기상 `unreadable_*`/`RN1_raw_values` | ADR-0007 (0값/결측), D-03·D-04 |

**한계.** 표본이 몇 시간뿐이라 드문 일은 안 보입니다. 숫자는 `rows`(표본 크기)와 같이 보고, 하루 이상 쌓인 뒤 다시 실행해 비교합니다. 수집 공백 구간(16:26~18:00)의 행은 없습니다.

## 5-1 ~ 5-4. Silver 잡

**무엇을 하나.** `data/bronze/<소스>` 5개 표를 스트림으로 읽어 `data/silver/<표>/` 에 Parquet 으로 쌓는다. 규칙은 ADR-0007, 설정은 `config/silver.yaml`.

| Silver 표 | 파티션 | 중복 제거 | 덧붙인 열 |
|---|---|---|---|
| `silver_traffic_speed` | `event_date` | 워터마크 10분, `dedup_key` | `dq_flags`(`speed_zero`·`speed_null`·`travel_time_zero`) |
| `silver_incident_observation` | `observed_date`(= `fetched_at` 의 KST 날짜) | 워터마크 10분(`fetched_at`), `(dedup_key, ingest_id)` — 호출 사이 반복은 **남긴다** | `lon`·`lat`(EPSG:5181→WGS84), `planned_duration_days`, `is_long_term`(7일 초과) |
| `silver_bus_position` | `event_date` | 워터마크 10분, `dedup_key` | |
| `silver_bus_arrival` | `event_date` | 워터마크 10분, `dedup_key` | `arrive_zero`·`arrive_null` |
| `silver_weather` (긴 형태) | `event_date` | 워터마크 3시간, `dedup_key` | `value_unparsed` |

모든 표 공통: Bronze 의 `_parse_ok` 가 참인 행만, `payload` 는 평탄화, `silver_processed_at`, `_kafka_partition/_kafka_offset`, `dq_flags`(`ingest_clock_skew` 포함).

### 켜기

```zsh
docker compose --profile core --profile streaming build spark-bronze
```

Silver 는 Bronze 와 **같은 이미지**를 쓴다(코드가 이미지에 들어 있음). 코드를 바꿨으면 이 빌드를 먼저 한다. 돌고 있는 Bronze 는 재시작되지 않는다.

```zsh
scripts/stack.sh up-silver
```

`up-silver` 는 `up --build --wait` 를 하므로 이미지가 바뀌었으면 Bronze 도 새 이미지로 다시 뜬다(체크포인트에서 이어감). 이를 피하려면 Silver 만 켠다:

```zsh
mkdir -p data/silver && docker compose --profile silver up -d --no-deps spark-silver
```

### 확인

Silver 가 따라잡는 데 몇 분 걸린다(Bronze 파일 약 8,300개/일, 한 번에 최대 1,000개씩). 진행 상황:

```zsh
docker logs --tail 20 urbanstream-spark-silver 2>&1 | grep -v -E "INFO|WARN"
```

```zsh
scripts/silver_verify.sh 2>&1 | grep -v -E "INFO|WARN|^\s+at |Suppressed"
```

`silver_verify` 가 보는 것: Bronze 전체 행 = 정상 행 + 읽기 불가 행(사유별), 키 단위 유실 0·초과 0, 돌발은 `(dedup_key, ingest_id)` 중복 0. Silver 가 뒤따라가는 구간(마지막 처리 `fetched_at` 에서 15분 안)은 비교에서 뺀다. 표마다 `미래 시각`(event_time 이 fetched_at 보다 60초 넘게 뒤)과 `시간 역전`(버스위치에서 같은 차량의 GPS 시각이 수집 순서대로 거꾸로)도 0 이어야 통과하고, 돌발 아래에는 변환된 경도·위도를 설명문과 나란히 보는 `[좌표 확인용]` 줄이 붙는다(사람이 확인, 통과 판정에는 안 씀). **종료 코드 0 이 통과.**

비교 기준이 "Silver 마지막 `fetched_at` −15분"이라, 데이터가 끊긴 직전 15분은 비교에서 빠진다.

### 알려진 한계

- 워터마크가 지난 뒤 같은 `dedup_key` 가 다시 오면 Silver 에 또 들어간다(`재통과(정보)` 열). 정상 운영에서는 0 에 가깝지만 Bronze 재시작·백필 뒤에는 생길 수 있다.
- 수집 공백(덮개를 닫은 잠자기 등)은 Silver 에서 "행이 없는 구간"으로만 보인다. 표시하는 일은 D-09.
- 도로소통은 링크 1개만 수집 중이라 `silver_traffic_speed` 는 사실상 한 줄 시계열이다 (D-40).
- Mac 컨테이너에서의 메모리·처리 속도는 아직 측정하지 않았다 (D-30, D-07).
