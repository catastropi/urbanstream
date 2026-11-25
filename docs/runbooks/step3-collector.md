# Step 3 런북 — 토픽 만들기, Collector 첫 실행 (사용자 Mac 에서)

이 환경에서는 실제 API 와 Kafka 에 붙을 수 없어서, 아래가 **첫 실행**입니다. 단계마다 기대 결과를 적었으니 다르면 출력 전체를 붙여넣어 주세요(키 값은 출력되지 않습니다).
프로젝트 루트(`urbanstream/`)에서, **`conda activate urbanstream`(Python 3.11)** 상태로 실행합니다. Docker Desktop 이 켜져 있어야 합니다.

## 0. 준비

```zsh
conda activate urbanstream
pip install -r requirements-dev.txt     # confluent-kafka, pyyaml 이 추가됨 (requirements.txt)
python -m pytest -q                      # 전부 통과해야 함 (docker 없이도 통과)
scripts/stack.sh up && scripts/stack.sh check     # 스택이 떠 있어야 함 (8/8 통과)
```

`pip install` 에서 `confluent-kafka` 가 실패하면(휠이 없는 Python 버전 등) 오류 전체와 `python --version` 을 알려 주세요.

## 1. 토픽 만들기 (한 번만)

```zsh
python scripts/create_topics.py --plan     # 만들 내용 확인 (접속 안 함)
python scripts/create_topics.py            # 생성
python scripts/create_topics.py --list     # 확인
```

기대: `생성함` 7줄 (`urban.*` 6개 + `platform.audit`). 다시 실행하면 `이미 있음`. Kafka UI(`scripts/stack.sh up-dev` → http://127.0.0.1:8090)에서도 보입니다.

## 2. 호출 예산·설정 확인

```zsh
python -m collector status
```

기대: 5개 소스가 `0 / 한도` 로 보입니다. **계획 호출이 한도의 80% 를 넘으면 시작 자체가 거부**됩니다(설정 오류 메시지).

## 3. 한 사이클, Kafka 없이 (실제 API 는 호출)

```zsh
python -m collector once --dry-run
```

소스 5개를 한 번씩 호출합니다(호출 예산 5회 소비). 기대 (숫자는 시간에 따라 다름):

```
seoul_traffic:1220003800     ok  attempts=1 records=1 ...
seoul_incident               ok  attempts=1 records=10 ...
bus_position:100100118       ok  attempts=1 records=13 ...      (04시 이전이면 0대일 수 있음)
bus_arrival:100100118        ok  attempts=1 records=190~210 ...
kma_ncst:60,127              ok  attempts=1 records=8 ...
[dry-run] 발행했을 메시지 수: {...}
```

- `failed`/`permanent_failure` 가 있으면 `reason` 열(auth, param, malformed, network_error ...)을 알려 주세요. **인증 오류(auth)는 재시도하지 않고 15분간 해당 대상을 쉽니다.**
- 이때 `data/raw/<source>/<날짜>/` 에 원본(gz)이 쌓입니다. `gunzip -c <파일> | head -c 400` 으로 열어볼 수 있습니다.

## 4. 한 사이클, Kafka 로

```zsh
python -m collector once
```

끝에 `Kafka 전달 성공 N건 / 실패 0건` 이 나와야 합니다. 확인:

```zsh
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:19092 --topic platform.audit --from-beginning --max-messages 3 --timeout-ms 10000
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:19092 --topic urban.bus.position.v1 --from-beginning --max-messages 2 --timeout-ms 10000 \
  --property print.key=true --property print.timestamp=true
```

기대: audit 은 JSON 한 줄씩(`outcome`, `n_records`, `budget_used`...), 버스위치는 `CreateTime:<ms>  <vehId>  {"event_id":...}`.

## 5. 계속 수집

상시 수집은 컨테이너로 돌린다(`docker-compose.yml` 의 `collector`). 호스트에서 수집기가 돌고 있으면 시작을 거부한다.

```zsh
scripts/stack.sh up-collector
```

```zsh
scripts/stack.sh logs collector
```

호스트에서 직접 돌리는 방법(문제 조사용. 컨테이너를 먼저 멈춘다):

```zsh
python -m collector run
```

- 로그가 호출마다 한 줄씩 나옵니다: `[bus_position:100100118] ok attempts=1 records=13 ... budget=12/1000`.
- 종료는 `docker compose --profile collector stop collector`, 호스트에서는 `Ctrl+C` (현재 사이클을 마치고 정리 후 종료, 오늘 예산 사용량 출력).
- 10~20분 돌려 보고, 다른 터미널에서 `python -m collector status` 와 `scripts/stack.sh stats` 를 확인하세요.
- 호출 1회의 총 제한시간은 15초입니다(`timeout_sec.total`). 넘으면 `TotalTimeout` 으로 기록되고 재시도합니다. 한 소스가 느려도 다른 소스의 간격은 유지됩니다(소스별 스레드). 이상하면 `python -m collector run --sequential` 로 비교해 보세요.
- 노트북이 잠들면 컨테이너도 같이 멈춥니다(Docker VM 이 멈춘다). 장시간 수집하려면 잠자기 방지가 필요합니다(`mac-on-off.md` 4절).

## 6. 문제 해결

| 증상 | 원인 | 해결 |
|---|---|---|
| `설정 오류: ... daily_budget ...` | 계획 호출이 한도의 80% 초과 | `config/sources.yaml` 의 `interval_sec` 늘리기 |
| `설정 오류: .env 에 ... 가 없습니다` | 키 누락 | `.env` 의 `SEOUL_API_KEY` / `DATA_GO_KR_KEY` |
| `Kafka 에 접속하지 못했습니다` | 스택 꺼짐 | `scripts/stack.sh up` |
| 메시지가 안 보임 + `Unknown topic` 류 오류 | 토픽 미생성 (자동 생성 꺼짐) | `python scripts/create_topics.py` |
| `circuit_open` 이 audit 에 반복 | 연속 실패 또는 인증 오류로 쉬는 중 | 원인(`status_kind`)을 고친 뒤 기다리거나 프로세스 재시작 |
| `budget_exhausted` | 오늘 호출 예산 소진 | `python -m collector status`. 한도가 실제로 더 크면 `daily_budget` 조정 (근거를 `budget_basis` 에 기록) |
| 같은 `dataTm` 이 여러 번 나옴 | 콜렉터는 중복을 거르지 않음 (설계) | Step 5 Silver 에서 `dedup_key` 로 제거 |

## 다음

- [ ] 3번(dry-run)과 4번(Kafka) 출력 붙여넣기
- [ ] 5번을 10~20분 돌린 뒤 `status` 와 `stats` 붙여넣기
- [ ] Step 0 마무리 측정 결과 ([step0-closeout.md](step0-closeout.md)) — 오늘 낮이므로 지금 하기 좋습니다
- 그다음 Step 4(Raw/Bronze: Spark Structured Streaming). 이때 Spark 이미지 크기·메모리를 실측합니다 (ADR-0003 결정 7 참고)
