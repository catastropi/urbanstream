# ADR-0003 — Step 2 Docker Compose 인프라: 범위, 이미지, 포트, 메모리

- 상태: 확정 (Step 2, 2026-11-02). 사용자 Mac 에서 기동·점검·메모리 측정을 마쳐 닫았다
- 관련: `docker-compose.yml`, `scripts/stack.sh`, `docs/runbooks/step2-stack.md`

## 배경

Blueprint W절은 core / streaming / dev / monitoring / optional 이라는 profile 구성을 정했고 "Mac + Docker Desktop 의 동시 기동 메모리를 먼저 측정해서 profile 조합을 정한다" 를 결정 사항으로 남겼다. Blueprint 본문이 없으므로 이 ADR 이 Step 2 의 설계 기록이다.

## 결정 1 — 지금 있는 것만 넣는다

Step 2 의 compose 에는 `postgres`(PostGIS), `kafka`(KRaft), `kafka-ui`(dev) 3개만 둔다. collector(Step 3), Spark(Step 4~5), Airflow(Step 7), API(Step 9)는 이미지(Dockerfile)가 아직 없다. 없는 서비스를 미리 적으면 `up` 이 실패하거나, 검증하지 못한 설정이 쌓인다. 각 서비스는 만드는 단계에서 같은 파일에 추가한다.

## 결정 2 — 이미지 (모두 Apple Silicon/Intel 둘 다 지원)

| 서비스 | 이미지 | 이유 |
|---|---|---|
| postgres | `imresamu/postgis:17-3.6.1-bookworm` | 공식 `postgis/postgis` 의 arm64 지원 여부를 확인하지 못했다. 이 이미지는 Docker Hub 페이지에서 amd64·arm64 둘 다 표기를 확인했다(2026-11-02). 기본 DB 에 PostGIS 확장이 설치돼 있다. PostgreSQL 17 + PostGIS 3.6.1 |
| kafka | `apache/kafka:3.9.2` | Apache 공식 이미지, KRaft 지원, amd64·arm64 확인. 최신(`4.2.0`)이 나와 있지만 .0 릴리스라 안정 계열 3.9 를 택했다. 바꾸려면 `.env` 의 `KAFKA_IMAGE` |
| kafka-ui | `kafbat/kafka-ui:latest` | 원조 `provectuslabs/kafka-ui` 는 개발이 멈췄고 Kafbat 이 이어받았다. 개발용 도구라 `latest` 를 허용(핵심 서비스는 `latest` 금지, 테스트로 강제) |

핵심 이미지는 태그를 고정한다. `latest` 는 어느 날 갑자기 바뀌어 "어제는 됐는데" 를 만든다 (원칙 '재처리 가능', 재현성).

## 결정 3 — Kafka 는 KRaft 단일 노드, 리스너 2개

- ZooKeeper 가 없다: 컨테이너 1개, 메모리 절약. Kafka 4 에서는 ZooKeeper 가 제거됐다.
- 리스너 `INTERNAL://kafka:19092`(컨테이너끼리)와 `EXTERNAL://localhost:9092`(내 Mac). 이유: 광고(advertised) 주소는 하나만 안내되므로, 같은 주소로 컨테이너와 호스트를 모두 만족시킬 수 없다. 호스트에서 돌리는 개발 코드(Collector 실행, 테스트)는 `localhost:9092`, 컨테이너 안(Spark, Airflow, UI)은 `kafka:19092` 를 쓴다.
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`: 토픽 이름 오타가 조용히 새 토픽을 만드는 일을 막는다. 토픽은 Step 3(H절)에서 코드로 명시적으로 만든다.
- 단일 브로커이므로 내부 토픽 복제 수를 1로 둔다 (안 하면 기동 후 컨슈머 그룹이 동작하지 않는다).
- 힙 `-Xms256m -Xmx512m`: 개발용. 처리량 시험 때 올린다.

## 결정 4 — 포트

| 서비스 | 호스트 포트 | 이유 |
|---|---|---|
| postgres | **5433** | 5432 는 로컬/첫 프로젝트의 Postgres 와 겹치기 쉽다 |
| kafka | 9092 | 관례 |
| kafka-ui | **8090** | 8080 은 이후 Airflow 가 쓴다 |

모든 포트는 `127.0.0.1` 에만 연다(테스트로 강제). 와이파이의 다른 기기에서 DB/Kafka 에 접근할 수 없다. 인증이 없는 PLAINTEXT Kafka 를 외부에 열지 않기 위함이다.

## 결정 5 — 비밀과 설정은 `.env`, 필수 값은 없으면 실패

- `POSTGRES_PASSWORD`, `KAFKA_CLUSTER_ID` 는 compose 에서 `${VAR:?메시지}` 로 필수 처리한다. 값이 없으면 "먼저 init-env 를 실행하세요" 라는 메시지와 함께 기동을 거부한다. 기본 비밀번호(`postgres` 같은)로 조용히 뜨는 것보다 낫다.
- `scripts/stack.sh init-env` 가 `.env` 에 없는 항목만 추가하고 비밀 값은 무작위로 만든다. 이미 있는 값은 건드리지 않고, 값은 출력하지 않는다.
- `.env.example` 에는 비밀번호를 적지 않는다(빈 값).
- API 키(`SEOUL_API_KEY` 등)는 컨테이너에 전달하지 않는다. compose 가 참조하는 변수만 컨테이너에 들어간다.

## 결정 6 — 데이터는 이름 있는 볼륨, 서버 시간대는 기본(UTC) 유지

- `postgres-data`, `kafka-data` 이름 있는 볼륨: `down` 으로 지워지지 않는다. 지우려면 `stack.sh reset`(확인 질문)이 필요하다.
- **정정 1 (2026-11-03): Kafka 데이터가 볼륨에 저장되지 않고 있었다.** `docker system df -v` 에서 `urbanstream_kafka-data` 가 0B 인데 kafka 컨테이너 쓰기 공간이 1.35GB 였고, `find` 로 확인하니 토픽 디렉터리가 `/tmp/kafka-logs/urban.traffic.speed.v1-0` (이미지 기본 로그 위치)에 있었다. 볼륨은 `/var/lib/kafka/data` 에 연결했지만 `KAFKA_LOG_DIRS` 를 지정하지 않았기 때문이다. 그래서 위 문장은 볼륨에 대해서만 참이었고, **`down` 으로 컨테이너가 지워지면 토픽·메시지·오프셋이 사라지는 상태**였다 (Step 2 의 8/8 점검에는 재시작 후 유지 여부가 없었다). 수정: `KAFKA_LOG_DIRS: /var/lib/kafka/data`, 그리고 "로그 경로가 볼륨 마운트 안에 있어야 한다"는 테스트(`test_compose_policy.py`). 적용하면 컨테이너를 다시 만들어야 하고, 그때 이전 컨테이너 안의 Kafka 데이터는 사라진다 (원본 응답은 `data/raw` 에 있어 재발행할 수 있다). 되돌리기: 환경변수 한 줄을 지운다 (되돌리면 다시 볼륨 밖에 저장된다).
- Postgres 시간대는 기본값(UTC)을 그대로 둔다. 모든 시각은 `timestamptz` 로 저장하고(계약의 시각은 KST 오프셋이 붙은 값), 화면(Tableau/대시보드)에서 KST 로 보여준다. 서버 시간대를 KST 로 바꾸면 `timestamp`(오프셋 없는 값)를 쓸 때 조용히 틀리는 위험이 생긴다.
- `docker compose` 의 프로젝트 이름은 `urbanstream` 으로 고정했다(폴더 이름이 바뀌어도 컨테이너·볼륨 이름이 같다).

## 결정 7 — 메모리: 측정값으로 확정 (2026-11-02, 사용자 Mac)

환경: Apple Silicon(aarch64), Docker Desktop VM 메모리 7,851 MiB, CPU 4개, Docker Compose v2.10.2.
유휴 상태(데이터 없음, core 만 기동) 실측:

| 컨테이너 | 사용량 | 한도 | 사용률 |
|---|---|---|---|
| kafka | 318 MiB | 1 GiB | 31% |
| postgres | 59 MiB | 768 MiB | 8% |
| 합계 | 약 377 MiB | | VM 의 약 4.8% |

- 한도는 그대로 둔다 (kafka 1 GiB: 힙 512m + 오프힙 여유, postgres 768m). 부하가 걸리면 늘 수 있으므로 Step 3 수집을 켠 뒤 다시 측정한다.
- **Spark 단계(Step 4~5)에 주는 의미:** VM 7.85 GiB 에서 core 가 0.4 GiB 를 쓰고, 남는 약 7.4 GiB 를 Spark·Airflow·API·Tableau 연동이 나눠 써야 한다. Spark 드라이버는 JVM 이라 잡 하나가 보통 1 GiB 이상을 쓴다. 원래 계획(Spark 잡 4개를 각각 컨테이너로)은 4~6 GiB 가 될 수 있어 Airflow(웹서버+스케줄러+메타DB)와 함께 돌리기엔 빠듯하다. **Step 4 에서 실측하기 전까지는 '스트리밍 잡을 한 Spark 애플리케이션 안의 여러 쿼리로 묶는 안'을 기본 후보로 둔다.** 이것은 추정이며 Step 4 에서 Spark 이미지를 올린 뒤 측정으로 확정한다.

## 검증 상태 (정직하게)

| 확인한 것 | 방법 |
|---|---|
| compose 문법, 필수 변수 누락 시 실패, 포트·한도 인터폴레이션 | `docker compose config` 로 이 환경에서 확인 |
| 규칙(127.0.0.1, 태그 고정, 비밀 없음, 한도, healthcheck) | `tests/unit/test_compose_policy.py` |
| `.env` 채우기, 점검 스크립트 흐름 | 단위 테스트 + 가짜 `docker` 명령 테스트 |
| 실제 컨테이너 기동, 이미지 pull, healthcheck 통과, Kafka 왕복 | **사용자 Mac 에서 확인함 (2026-11-02): `stack.sh check` 8/8 통과, PostGIS 3.6.1 / PostgreSQL 17.7, timezone Etc/UTC** |
