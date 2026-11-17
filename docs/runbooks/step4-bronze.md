# Step 4 런북 — Bronze(Spark) 첫 실행과 검증 (사용자 Mac 에서)

이 환경에서는 Docker 를 띄울 수 없어서, 아래가 **이미지의 첫 빌드와 첫 실행**입니다. 단계마다 기대 결과를 적었으니 다르면 출력 전체를 붙여넣어 주세요.
프로젝트 루트(`urbanstream/`)에서 **`conda activate urbanstream`** 상태로, Docker Desktop 이 켜진 채로 실행합니다. 명령은 한 줄씩 복사하세요.
결정 근거는 `docs/adr/0005-bronze-spark.md` 입니다.

## 0. 준비 (적용 → 테스트)

zip 은 **터미널에서만** 적용합니다 (Finder 로 폴더를 덮어쓰면 다른 파일이 사라진 적이 있습니다).

```zsh
unzip -o ~/Downloads/urbanstream_full.zip -d ..
```

```zsh
python -m pytest -q
```

기대: **`174 passed, 2 skipped`**. skipped 2개는 pyspark 가 필요한 테스트라서 이 환경(pyspark 없음)에서 건너뛰는 것이 정상입니다 (4번에서 컨테이너 안에서 돌립니다). 숫자가 다르면 일부 파일만 복사된 것일 수 있으니 출력 전체를 알려 주세요.

수집기(`caffeinate -i python -m collector run`)와 core 스택(`scripts/stack.sh check` 8/8)은 켜져 있어야 합니다. 토픽에 메시지가 있어야 Bronze 에 쌓입니다.

## 1. 이미지 빌드 + 기동 (첫 빌드는 수 분, 이미지 크기는 1~2 GB 정도로 추정)

디스크 여유부터 확인합니다 (Mac 33 GiB 여유였음).

```zsh
docker system df
```

```zsh
scripts/stack.sh up-streaming
```

기대: 베이스 이미지(`apache/spark`, 압축 약 750 MB)를 받고, `apt-get`(python3-yaml, python3-pytest)과 커넥터 해석이 진행된 뒤 마지막에 `spark-bronze ... healthy`. 빌드 로그의 마지막 부분에 jar 4개 이름이 보여야 합니다:

- `spark-sql-kafka-0-10_2.13-4.1.3.jar`
- `spark-token-provider-kafka-0-10_2.13-4.1.3.jar`
- `kafka-clients-….jar`
- `commons-pool2-….jar`

**빌드가 실패하면 가장 의심되는 곳은 커넥터 jar 복사 단계**입니다 (이 단계는 설계 환경에서 확인하지 못했습니다 — Maven Central 접근 불가). 그 단계의 오류 줄 전체(`cp: cannot stat …` 등)를 붙여넣어 주세요.

## 2. 도는지 확인

```zsh
docker compose --profile streaming logs --tail=40 spark-bronze
```

기대: `Bronze 시작: bootstrap=kafka:19092 starting=earliest trigger=60s ...` 와 토픽 7개 줄(`계약 스키마로 파싱` 5개, `원문만 보존` 2개). `ERROR`·`Exception` 이 없어야 합니다.

Spark UI: http://127.0.0.1:4040 → Structured Streaming 탭에 쿼리 7개(`bronze_*`)가 보입니다.

1~2분 뒤 파일이 생깁니다 (트리거 60초).

```zsh
ls data/bronze
```

기대: `audit  bus_arrival  bus_position  dlq  kma_ncst  seoul_incident  seoul_traffic` (일부는 해당 토픽에 메시지가 있을 때만. `dlq` 는 계약 위반이 없으면 안 생길 수 있음).

```zsh
ls data/bronze/bus_position
```

기대: `_spark_metadata` 와 `event_date=2026-11-03` 같은 폴더.

**권한 오류**(`Permission denied`, 쿼리 실패)가 나면: 컨테이너가 비루트(uid 185)라서 마운트 폴더에 쓰지 못하는 경우입니다.

```zsh
chmod -R a+rwX data/bronze data/checkpoints
```

그다음 `docker compose --profile streaming restart spark-bronze`.

## 3. 메모리 실측 (ADR-0005 결정 3 의 추정을 확정)

수집기·Kafka·Postgres 가 켜진 상태에서, 기동 직후와 30분 뒤에 한 번씩 측정합니다.

```zsh
scripts/stack.sh stats
```

기대(추정): `urbanstream-spark-bronze` 가 한도 1.5 GiB 아래. **한도의 90% 를 넘거나 재시작(`docker compose ps` 에서 RESTARTS)이 보이면** 결과를 알려 주세요 (힙/한도 조정). 결과를 ADR-0005 "실측 기록" 표에 적습니다.

작은 파일 수 (하루 쌓인 뒤 다시 세어 비교):

```zsh
find data/bronze -name '*.parquet' | wc -l
```

## 4. 검증: Bronze 행 수 = Kafka, 중복·유실 0 (완료 기준 1)

```zsh
scripts/bronze_verify.sh
```

별도 JVM 이 하나 더 떠서 약 1 GiB 를 잠시 더 씁니다. 기대: 표마다 `OK`, `중복 0`, `유실 0`, "Kafka 같은 구간" 건수 = Bronze offset 수. 마지막 줄 `전체: 통과`.

- `뒤처짐 N건`: 아직 Bronze 에 안 들어온 최근 구간(트리거 주기 안의 정상 지연)입니다. 수천 건 이상으로 계속 늘면 쓰기가 못 따라가는 것이니 알려 주세요.
- `시작 전 N건`: Bronze 가 시작되기 전에 이미 Kafka 에 있던 메시지가 아니라 **Bronze 최소 offset 앞의 구간**입니다. `earliest` 로 시작했다면 0 이어야 합니다.
- `파싱불가 N행`: `_parse_ok=false` 인 행. 0 이 기대값입니다 (0 이 아니면 어떤 소스인지 알려 주세요 — 계약과 메시지가 어긋난 것).

Spark 테스트를 **운영과 같은 이미지(Java 17)** 안에서 돌립니다 (설계 환경에서는 Java 21 로 46개 통과). 커넥터 jar 가 이미지에 있는지 확인하는 테스트 1개가 여기서 처음 실행됩니다.

```zsh
scripts/spark_test.sh
```

기대: **`57 passed`** (사용자 Mac 실측, 2026-11-03). 실패(`F`)가 없는지가 핵심입니다. 설계 환경(Java 21, 커넥터 jar 없음)에서는 같은 테스트가 jar 확인 1개를 건너뛰었습니다. `Unknown config option: pythonpath` 경고는 컨테이너의 pytest 가 오래된 버전이라서 나오는 무해한 안내입니다.

## 5. 복구 시험: 죽였다 살려도 중복·유실 없음 (완료 기준 2)

일부러 비정상 종료(SIGKILL)시킵니다. 배치 도중에 끊기는 경우까지 시험하려고 `stop` 이 아니라 `kill` 을 씁니다.

```zsh
docker kill urbanstream-spark-bronze
```

Docker 가 자동으로 다시 켤 수도 있습니다. 어느 쪽이든 아래 명령으로 켜진 상태를 보장합니다.

```zsh
COMPOSE_PROFILES=core,streaming docker compose up -d --wait spark-bronze
```

1~2분 기다린 뒤:

```zsh
scripts/bronze_verify.sh
```

기대: 4번과 같이 `중복 0`, `유실 0`, `전체: 통과`. **중복이 나오면 이 Step 의 가장 중요한 결과이니 출력 전체를 알려 주세요.** 이 시험은 설계 환경에서 할 수 없었습니다 (이 환경의 재시작 시험은 `stop()` 후 재시작까지만 확인).

(수집기가 도는 동안 시험하세요. 메시지가 계속 들어와야 "죽은 사이에 쌓인 구간을 이어 읽는지"가 보입니다. 더 확실히 하려면 컨테이너를 3~5분 내려 둔 뒤 켜 보세요 — 밀린 양을 `maxOffsetsPerTrigger` 단위로 나눠 읽습니다.)

## 6. 끄기 / 되돌리기

Spark 만 끄고 core 는 유지:

```zsh
docker compose --profile streaming stop spark-bronze
```

전체 중지(데이터 유지)는 `scripts/stack.sh down`. 설정 되돌리기는 `.env` 의 `BRONZE_*`, `SPARK_*` 항목을 지우면 기본값이 됩니다 (`.env.example` 참고). 쓰기 주기를 늘려 파일 수를 줄이려면:

```zsh
echo 'BRONZE_TRIGGER_SEC=300' >> .env
```

(적용: `scripts/stack.sh up-streaming`)

## 7. ⚠ Kafka 를 지우거나 새로 만들었을 때 (`stack.sh reset`, 볼륨 삭제, 7-1 같은 재생성)

Kafka offset 은 0 부터 다시 시작합니다. 그대로 두면 (a) 체크포인트의 offset 이 새 Kafka 보다 커서 쿼리가 `failOnDataLoss` 로 실패하거나 (b) 예전 Bronze 와 offset 이 겹쳐 `bronze_verify` 가 중복으로 판정합니다. **Kafka 를 지웠다면 Bronze 도 같이 치워야 합니다.** 지우기 전에 옮겨 두는 쪽을 권합니다 (원본 데이터는 `data/raw` 에 따로 있습니다).

```zsh
docker compose --profile streaming stop spark-bronze
```

```zsh
mkdir -p data/_old && mv data/bronze data/_old/bronze_$(date +%Y%m%dT%H%M%S) && mv data/checkpoints data/_old/checkpoints_$(date +%Y%m%dT%H%M%S)
```

```zsh
mkdir -p data/bronze data/checkpoints
```

그다음 `scripts/stack.sh up-streaming`.

## 8. ⚠ Mac 을 72시간보다 오래 꺼 뒀다 켰더니 `spark-bronze` 가 unhealthy 일 때 (`OffsetOutOfRange`)

**무슨 일인가.** Kafka 는 메시지를 72시간만 보관합니다(`config/topics.yaml`, DLQ·audit 은 7일). 그보다 오래 꺼 두면 Bronze 체크포인트가 가리키는 offset 의 데이터가 이미 지워져 있고, 설계대로(`failOnDataLoss=true`, `streaming/bronze.py`) 쿼리가 유실을 숨기지 않고 **실패**합니다. 11-12 17:05 에 끄고 11-16 에 켰을 때 실제로 일어났습니다(DEFERRED D-255). 꺼 둔 동안은 수집기도 서 있었으므로 **그 구간의 데이터는 어디에도 없습니다.** 되살릴 것은 "앞으로"입니다.

**확인.** 로그에 `OffsetOutOfRange`(또는 `data may have been lost`)가 있고, 어느 토픽이 문제인지 본다.

```zsh
docker compose --profile streaming logs --tail 400 spark-bronze | grep -i -E "OffsetOutOfRange|may have been lost|urban\.|platform\."
```

토픽 → Bronze 표(폴더 이름): `urban.bus.arrival.v1`→`bus_arrival`, `urban.bus.position.v1`→`bus_position`, `urban.traffic.incident.v1`→`seoul_incident`, `urban.traffic.speed.v1`→`seoul_traffic`, `urban.weather.observation.v1`→`kma_ncst`[추정: 이름 대응], `urban.dlq.v1`→`dlq`, `platform.audit`→`audit`.

**⚠ 가장 중요한 함정.** 체크포인트만 옮기면 안 됩니다. Spark 의 파일 싱크는 `data/bronze/<표>/_spark_metadata` 에 마지막으로 커밋한 batch id(예: 2034)를 기억하고, **그 번호 이하의 새 batch(새 체크포인트가 만든 72 같은 작은 번호)를 "이미 처리됨"으로 보고 조용히 건너뜁니다.** 쿼리는 건강해 보이는데 새 파일이 안 생깁니다(11-16 에 실제로 겪음). 그래서 **체크포인트와 `_spark_metadata` 를 같이** 옮깁니다. 옮긴 곳(`data/_old/`)에는 원본이 남으니 필요하면 되돌릴 수 있습니다.

**복구 (문제가 난 표마다, 예: 3개).**

```zsh
docker compose --profile streaming stop spark-bronze
```

```zsh
mkdir -p data/_old
```

표 이름만 바꿔 한 번에 하나씩(아래는 `bus_arrival` 예). 한 줄이 길어도 그대로 한 줄로 실행한다.

```zsh
t=bus_arrival; mv data/checkpoints/bronze/$t data/_old/ckpt_${t}_$(date +%m%d%H%M); mv data/bronze/$t/_spark_metadata data/_old/meta_${t}_$(date +%m%d%H%M)
```

```zsh
scripts/stack.sh up-silver
```

**성공 확인.** 몇 분 뒤 오늘 날짜의 새 폴더·파일이 생기고, Discord 에 `해소` 알림이 온다.

```zsh
ls data/bronze/bus_arrival | tail -3
```

**하지 말 것.** 체크포인트만 옮기기. `data/bronze` 전체 삭제(이미 쌓인 Bronze 를 잃는다). `stack.sh reset`.

**예방.** 72시간 넘게 끌 일이 있으면 미리 알아 둔다 — 보관을 7일로 늘리는 선택(D-256, 디스크 더 사용)이 있다. 끄는 날짜·켜는 날짜는 `mac-on-off.md`.

## 알려진 한계 (정직하게)

- 이 Step 의 이미지 빌드, 실제 Kafka 연동, 배치 도중 kill 복구, 메모리는 **사용자 Mac 에서의 첫 실행이 검증**입니다 (ADR-0005 "검증 상태").
- Bronze 는 Kafka 가 만들어진 시점부터입니다. 그 이전 구간(`data/raw` 에만 있는 15:06~Kafka 재생성 전)은 `replay_raw.py` 를 만들 때 채웁니다 (아직 없음).
- `docker compose --profile streaming logs` 에는 Spark 의 `WARN` 이 일부 나옵니다 (`setLogLevel("WARN")`). `ERROR` 만 문제입니다.
