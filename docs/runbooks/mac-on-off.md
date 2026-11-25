# Mac 끄기·켜기 런북 (Step 12 전, Mac 에서 스택을 돌리는 동안)

Mac 에서 실행하는 순서. 비밀 값은 git·이슈·메신저 어디에도 붙이지 않는다. 근거: DEFERRED D-222(Mac 절전), D-211·D-212(Airflow OOM), D-214(재시작 정책).

## 0. 먼저 알아둘 것
- **계속 켜 둘 필요는 없다.** 끄고 켜는 절차만 지키면 된다. 꺼 둔 시간만큼 수집에 **빈 구간**이 생기고, 공공 API 는 실시간 값이라 **나중에 채울 수 없다.**
- **스택이 도는 동안 덮개를 닫거나 Mac 을 재우면 안 된다.** Mac 이 잠들면 Docker VM·수집기가 같이 멈추고, 깨어난 직후의 몰림으로 Airflow 가 작업을 고아로 처리해 컨테이너가 죽었다(11-07, 11-09). `caffeinate -i` 는 "가만히 둬서 자는 것"만 막고 **덮개를 닫아서 자는 것은 못 막는다.** 켜 둘 때는 전원 연결 + 덮개 열기.
- 장시간 수집·가동 증거는 Mac 이 아니라 Step 12(클라우드 VM)에서 쌓는다.

## 1. 끌 때

1. 수집기를 먼저 멈춘다. 먼저 멈추면 이미 들어간 데이터는 Bronze·Silver 로 정상 처리된다. 몇 분 기다리면 더 안전하지만 필수는 아니다(메시지는 Kafka 에 7일 남고 Spark 는 체크포인트에서 이어 받는다).

```zsh
docker compose --profile collector stop collector
```

(예전처럼 호스트에서 돌리고 있으면 그 터미널에서 Ctrl+C)

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

4. **마지막에** 수집기를 시작한다(Kafka 가 먼저 떠 있어야 한다). 컨테이너로 돈다. 호스트에서 수집기가 돌고 있으면 시작을 거부한다(예산 파일을 같이 쓰기 때문).

```zsh
scripts/stack.sh up-collector
```

로그와 호출 예산 확인(예산은 호스트에서 돌던 때와 같은 `data/collector/budget.json` 을 이어 쓴다):

```zsh
scripts/stack.sh logs collector
```

```zsh
python -m collector status
```

컨테이너가 안 될 때의 예전 방법: `caffeinate -i python -m collector run` (컨테이너를 먼저 멈춘다).

## 3. 켠 뒤 알아둘 것
- **Airflow 는 꺼진 동안 못 돈 실행을 따라잡지 않는다.** 모든 DAG 가 `catchup=False` — 꺼져 있던 시간대의 Gold·마트는 비어 있다. 필요하면 `backfill` DAG 로 수동 보충(HANDOFF 7장).
- **켠 직후 몇 분은 Discord 에 적재 정체·소스 down 알림이 올 수 있다.** 보통 곧 "해소" 가 따라온다. 20~30분 지나도 해소가 안 오면 `scripts/stack.sh logs spark-bronze` 등으로 본다.
- DAG 일시정지 상태는 Airflow DB(볼륨)에 남는다 [추정]. Airflow UI(http://127.0.0.1:8080)에서 DAG 가 켜져 있는지 한 번 본다.
- **Docker Desktop·Mac 재시작 후 컨테이너는 자동으로 켜지지 않는다**(재시작 정책이 `on-failure`, D-214). 위 2번을 다시 한다.
- 컨테이너가 죽어 있으면 **재생성하기 전에** `docker inspect -f '{{.Name}} 재시작횟수={{.RestartCount}} OOM={{.State.OOMKilled}} 종료={{.State.FinishedAt}}' <컨테이너>` 로 먼저 기록한다(`up-*` 가 컨테이너를 다시 만들면 정보가 사라진다).
- 하루 중 수집이 비어 있는지는 `ls data/bronze/bus_position` 의 `event_date=...` 폴더 파일 수로 본다(비어 있는 날 = Mac 이 잠든 날).

## 4. 덮개를 닫은 채 돌려야 할 때 (부득이한 경우만)
**기본은 여전히 "스택이 도는 동안 덮개를 닫지 않는다"(0절).** 11-16~17 에 덮개를 하루 가까이 덮어 둔 채 켜 보니 깨어난 직후 Airflow scheduler 가 한도(4 GiB)에 닿았다(D-257). 꼭 닫아야 하면:
- 전원 어댑터를 연결하고, 통풍이 되는 곳에 둔다(닫힌 Mac 은 열이 잘 안 빠진다 [추정]). 침대·이불 위 금지. 외부 모니터를 연결하면 덮개를 닫아도 절전하지 않는 경우가 있다 [추정, 내 Mac 에서는 확인 안 함].
- 닫기 전에 절전을 끈다(관리자 암호 필요). **끝나면 반드시 되돌린다.**

```zsh
sudo pmset -a disablesleep 1
```

```zsh
sudo pmset -a disablesleep 0
```

- 첫 명령은 닫기 전, 둘째는 연 뒤. 상태 확인은 `pmset -g | grep -i sleep`.
- **덮개를 연 채 화면만 꺼지는 것은 괜찮다.** 전원이 연결돼 있으면 시스템 설정의 '디스플레이가 꺼져 있을 때 자동으로 잠자기 방지'를 켜 두거나, 터미널 하나에서 `caffeinate -i` 를 띄워 둔다(수집기가 컨테이너라 예전처럼 수집기 명령에 붙일 수 없다). 문제는 덮개를 닫아서 자는 것뿐이다.
- 이 방식으로 하루 이상 돌려 안정적이었다는 **실측은 없다**(D-260). 처음에는 `scripts/stack.sh stats` 로 scheduler 메모리를 자주 본다.

## 5. 7일보다 오래 꺼 뒀다 켰을 때
Kafka 보존 7일(11-19 까지는 72시간)이 지나서 `spark-bronze` 가 `OffsetOutOfRange` 로 unhealthy 가 될 수 있다. 그 구간의 데이터는 어디에도 없다(수집기도 꺼져 있었다). 켠 뒤 `up-silver` 가 unhealthy 로 끝나면 먼저 이것부터 본다.

```zsh
python scripts/bronze_recover.py status
```

**복구는 `docs/runbooks/step4-bronze.md` §8** (`bronze_recover.py reset`). 체크포인트나 `_spark_metadata` 를 손으로 옮기지 않는다.

추가로 긴 정지 뒤에는: `up-airflow` 직후 scheduler 메모리(`scripts/stack.sh stats`)를 본다. Airflow 는 `catchup=False` 라 못 돈 실행을 따라잡지 않지만, 깨어난 직후 몇 시간치 `gold_intraday` 가 몰릴 수 있다. 4 GiB 에 가까워지면 scheduler 를 멈춘다(`docker compose stop airflow-scheduler`, 안 멈추면 `docker ps` 로 이름을 보고 `docker kill`). `silver_reconcile_daily`(04:30)의 `silver_verify` 는 최근 1일 구간만 비교하므로 정지 길이와는 무관하다(D-258). 다만 구간을 잘라도 30분을 넘기면 시간 초과이니 Mac 실측 결과를 D-258 에서 확인한다.

