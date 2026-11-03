# Step 2 런북 — Docker 스택 기동과 검증 (사용자 Mac 에서 실행)

이 환경에서는 컨테이너를 실제로 띄워 볼 수 없어서, 아래 순서가 **첫 실행**입니다. 막히면 출력 전체(비밀번호는 출력되지 않음)를 붙여넣어 주세요.
모든 명령은 프로젝트 루트(`urbanstream/`)에서 실행합니다. Docker Desktop 이 켜져 있어야 합니다.

## 1. 준비

```zsh
conda activate urbanstream
pip install -r requirements-dev.txt        # pyyaml, pytest 추가됨
python -m pytest -q                        # 88 passed 가 나오는지 (이 단계는 docker 없이도 통과)
docker info --format '{{.Architecture}} / 메모리 {{.MemTotal}}'
```

마지막 줄의 아키텍처(`aarch64` = Apple Silicon, `x86_64` = Intel)와 메모리를 알려 주세요.

## 2. `.env` 채우기 (한 번만)

```zsh
scripts/stack.sh init-env
```

- `.env` 에 `COMPOSE_PROFILES`, `POSTGRES_*`, `KAFKA_*` 항목이 **없는 것만** 추가됩니다. 기존 API 키 줄은 그대로입니다.
- `POSTGRES_PASSWORD` 와 `KAFKA_CLUSTER_ID` 는 자동으로 만들어집니다(값은 출력되지 않음).
- 5433, 8090 포트가 이미 다른 프로그램에 쓰이면 `.env` 의 `POSTGRES_PORT`, `KAFKA_UI_PORT` 를 바꾸세요.

## 3. 기동

```zsh
scripts/stack.sh up
```

- 처음에는 이미지 두 개(PostGIS 약 200 MB, Kafka 약 220 MB)를 받느라 몇 분 걸립니다.
- 끝나면 `postgres`, `kafka` 가 `healthy` 로 보여야 합니다. (kafka 는 JVM 기동 때문에 `start_period` 40초 + 수십 초 걸릴 수 있습니다)
- 시간 초과로 실패하면: `scripts/stack.sh logs kafka` 의 마지막 줄을 알려 주세요.

## 4. 점검

```zsh
scripts/stack.sh check
```

기대 결과(전부 PASS):

```
PASS  container:postgres        state=running health=healthy
PASS  container:kafka           state=running health=healthy
PASS  host-port:postgres        127.0.0.1:5433
PASS  host-port:kafka           127.0.0.1:9092
PASS  postgres:postgis          postgis=3.6.1 | postgres=17.x | timezone=Etc/UTC
PASS  kafka:external-listener   EXTERNAL(광고 주소 localhost) 리스너 응답
PASS  kafka:create-topic        stack-check-tmp
PASS  kafka:produce-consume     메시지 1건 왕복 확인
```

(임시 토픽 `stack-check-tmp` 는 점검 후 지워집니다.)

## 5. 메모리 측정 (ADR-0003 결정 7을 닫는 데 필요)

```zsh
scripts/stack.sh stats          # core 만 떠 있는 상태
scripts/stack.sh up-dev         # kafka-ui 까지
scripts/stack.sh stats          # dev 포함 상태
```

`data/stack/stats_*.txt` 두 개의 내용을 붙여넣어 주세요. Docker Desktop 의 메모리 할당(Settings → Resources)도 같이 알려 주시면 Spark 단계의 조합을 정할 수 있습니다.

## 6. 눈으로 확인 (선택)

- Kafka UI: http://127.0.0.1:8090 (`up-dev` 후). 클러스터 `urbanstream-local` 이 보이면 정상. 토픽은 아직 없습니다(Step 3 에서 만듭니다).
- Postgres: `docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'` → `\dx` 에 `postgis` 가 보이면 정상. (DBeaver 등 외부 도구는 `127.0.0.1:5433`, 사용자·DB 는 `.env` 값, 비밀번호는 `.env` 의 `POSTGRES_PASSWORD`)

## 7. 끄기 / 지우기

| 하고 싶은 것 | 명령 | 데이터 |
|---|---|---|
| 잠시 끄기 | `scripts/stack.sh down` | 유지 |
| 다시 켜기 | `scripts/stack.sh up` | 이어서 사용 |
| 처음 상태로 | `scripts/stack.sh reset` (yes 입력) | **삭제** |

## 문제 해결

| 증상 | 원인 | 해결 |
|---|---|---|
| `POSTGRES_PASSWORD 가 .env 에 없습니다` | init-env 미실행 | `scripts/stack.sh init-env` |
| `port is already allocated` / `address already in use` | 5433/9092/8090 이 이미 사용 중 | `.env` 의 포트 변경 후 다시 `up` |
| `no matching manifest for linux/arm64` | 이미지가 해당 아키텍처를 지원하지 않음 | 어떤 이미지인지 알려 주세요 (`.env` 의 `POSTGRES_IMAGE`/`KAFKA_IMAGE` 로 교체 가능) |
| kafka 가 `unhealthy` / 재시작 반복 | 메모리 부족 또는 설정 오류 | `scripts/stack.sh logs kafka` 마지막 50줄, `docker info` 메모리 |
| `Cluster ID mismatch` / `meta.properties` 오류 | `KAFKA_CLUSTER_ID` 를 바꿨는데 기존 볼륨이 남아 있음 | 데이터가 필요 없으면 `scripts/stack.sh reset` |
| 호스트에서 `localhost:9092` 접속이 안 됨 | 컨테이너 안(`kafka:19092`)과 밖(`localhost:9092`) 주소가 다름 | 호스트 코드는 `localhost:9092`, 컨테이너 안 코드는 `kafka:19092` |

## 다음 (Step 3 로 가기 전에)

- [ ] `stack.sh check` 전체 PASS 결과 붙여넣기
- [ ] `stats` 결과 2개 + Docker Desktop 메모리 할당 알려 주기
- [ ] Step 0 마무리 측정 결과 ([step0-closeout.md](step0-closeout.md))

## 데이터가 볼륨에 저장되는지 확인 (2026-11-03 추가)

Kafka 로그가 `/var/lib/kafka/data`(볼륨)에 쌓여야 `down`/재생성 뒤에도 토픽과 메시지가 남는다. 토픽을 만든 뒤:

```zsh
docker compose exec kafka ls /var/lib/kafka/data
```

`urban.*` 토픽 디렉터리가 보여야 한다. 비어 있으면 로그가 볼륨 밖(`/tmp/kafka-logs`)에 쌓이는 것이다 (`docker-compose.yml` 의 `KAFKA_LOG_DIRS` 확인). `docker system df -v` 의 `urbanstream_kafka-data` 크기가 수집이 진행되며 커지는지도 볼 수 있다.

