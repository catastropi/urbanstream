# Mac 끄기·켜기 런북 (Step 12 전, Mac 에서 스택을 돌리는 동안)

**사용자가 Mac 에서 실행하는 순서입니다.** 비밀 값은 채팅·git 어디에도 붙이지 않습니다. 근거: DEFERRED D-222(Mac 절전), D-211·D-212(Airflow OOM), D-214(재시작 정책).

## 0. 먼저 알아둘 것
- **계속 켜 둘 필요는 없다.** 끄고 켜는 절차만 지키면 된다. 꺼 둔 시간만큼 수집에 **빈 구간**이 생기고, 공공 API 는 실시간 값이라 **나중에 채울 수 없다.**
- **스택이 도는 동안 덮개를 닫거나 Mac 을 재우면 안 된다.** Mac 이 잠들면 Docker VM·수집기가 같이 멈추고, 깨어난 직후의 몰림으로 Airflow 가 작업을 고아로 처리해 컨테이너가 죽었다(11-07, 11-09). `caffeinate -i` 는 "가만히 둬서 자는 것"만 막고 **덮개를 닫아서 자는 것은 못 막는다.** 켜 둘 때는 전원 연결 + 덮개 열기.
- 장시간 수집·가동 증거는 Mac 이 아니라 Step 12(클라우드 VM)에서 쌓는다.

## 1. 끌 때

1. 수집기가 도는 터미널에서 **Ctrl+C**. (먼저 멈추면 이미 들어간 데이터는 Bronze·Silver 로 정상 처리된다. 몇 분 기다리면 더 안전하지만 필수는 아니다 — 메시지는 Kafka 에 72시간 남고 Spark 는 체크포인트에서 이어 받는다.)

```zsh
scripts/stack.sh down
```

2. 데이터 볼륨(Postgres·Kafka)은 유지된다. **`scripts/stack.sh reset` 은 데이터를 지우니 쓰지 않는다.**
3. 그다음 Docker Desktop 을 종료하거나 Mac 을 재워도 된다.

## 2. 켤 때 (프로젝트 폴더, conda `urbanstream` 환경)

1. Docker Desktop 시작 후 고래 아이콘이 "running" 이 될 때까지 기다린다(약 1분).

```zsh
open -a Docker
```

2. 스택을 순서대로 올린다. `down` 이 전부 내렸으므로 각각 다시 올려야 한다. 이미지는 만들어져 있어 빌드는 짧다 [추정]. Airflow 메모리 한도(`.env` 의 `AIRFLOW_*_MEM_LIMIT`)는 유지된다.

```zsh
scripts/stack.sh up-silver
```

(Postgres·Kafka·Bronze·Silver)

```zsh
scripts/stack.sh up-airflow
```

```zsh
scripts/stack.sh up-monitor
```

```zsh
scripts/stack.sh up-dashboard
```

(API 와 대시보드)

3. 상태 확인. 전부 `running (healthy)` 이면 정상이다.

```zsh
scripts/stack.sh ps
```

```zsh
scripts/stack.sh check
```

4. **마지막에** 수집기를 시작한다(Kafka 가 먼저 떠 있어야 한다).

```zsh
caffeinate -i python -m collector run
```

다른 터미널에서 호출 예산 확인:

```zsh
python -m collector status
```

## 3. 켠 뒤 알아둘 것
- **Airflow 는 꺼진 동안 못 돈 실행을 따라잡지 않는다.** 모든 DAG 가 `catchup=False` — 꺼져 있던 시간대의 Gold·마트는 비어 있다. 필요하면 `backfill` DAG 로 수동 보충(HANDOFF 7장).
- **켠 직후 몇 분은 Discord 에 적재 정체·소스 down 알림이 올 수 있다.** 보통 곧 "해소" 가 따라온다. 20~30분 지나도 해소가 안 오면 `scripts/stack.sh logs spark-bronze` 등으로 본다.
- DAG 일시정지 상태는 Airflow DB(볼륨)에 남는다 [추정]. Airflow UI(http://127.0.0.1:8080)에서 DAG 가 켜져 있는지 한 번 본다.
- **Docker Desktop·Mac 재시작 후 컨테이너는 자동으로 켜지지 않는다**(재시작 정책이 `on-failure`, D-214). 위 2번을 다시 한다.
- 컨테이너가 죽어 있으면 **재생성하기 전에** `docker inspect -f '{{.Name}} 재시작횟수={{.RestartCount}} OOM={{.State.OOMKilled}} 종료={{.State.FinishedAt}}' <컨테이너>` 로 먼저 기록한다(`up-*` 가 컨테이너를 다시 만들면 정보가 사라진다).
- 하루 중 수집이 비어 있는지는 `ls data/bronze/bus_position` 의 `event_date=...` 폴더 파일 수로 본다(비어 있는 날 = Mac 이 잠든 날).
