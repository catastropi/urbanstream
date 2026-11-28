# 복구 런북 — 증상을 보고 칠 명령 (STEP 3B)

Mac 의 프로젝트 폴더(`~/PycharmProjects/urbanstream`)에서 conda `urbanstream` 환경으로 친다. 명령은 한 줄씩 복사해 붙인다.
결정 근거는 `docs/adr/0017-step3b-recovery.md`, 설정 값은 `config/recovery.yaml`. 훈련 기록은 `docs/recovery-drills.md`.

**무엇이 문제인지 모르겠으면 언제나 이것부터.** 읽기만 하고 아무것도 바꾸지 않는다. 문제마다 '지금 상태 한 줄 + 다음에 칠 명령 한 줄' 을 보여 준다.

```zsh
python scripts/recover.py diagnose
```

## 1. 증상 → 명령

| 증상 | 먼저 칠 명령 | 그다음 | 시나리오 |
|---|---|---|---|
| `docker ps` 가 멈춘 채 답이 없다, Docker Desktop 이 "stopping/starting" 에서 안 넘어간다, host_watch 가 "Docker 가 응답하지 않습니다" 를 보냈다 | `pkill -x Docker; pkill -f com.docker.backend` | `open -a Docker` → 고래 아이콘이 멈추면 `python scripts/recover.py resume` | R6 |
| Mac 을 켰다 / Docker Desktop 을 다시 켰다 (컨테이너가 남아 있다) | `python scripts/recover.py resume` | 끝에 `stack.sh check` 결과가 나온다. 실패한 줄의 안내를 따른다 | R6 |
| 컨테이너가 지워졌다(`docker ps -a` 에 없다) | `mac-on-off.md` 2-B (`scripts/stack.sh up-*`) | — | — |
| `spark-bronze` 가 unhealthy, 로그에 `OffsetOutOfRange` | `python scripts/recover.py bronze` (미리보기) | 3절 | R1 |
| `docker stop` 이 "is zombie and can not be killed" | `mac-on-off.md` 1-A | 그 뒤 `diagnose` 의 'init' 줄 | R7 |
| 버스 마트 DAG 가 "DQ block_mart 규칙이 실패" 로 바로 실패 | `python scripts/recover.py clock` | 4절 | R8 |
| Airflow scheduler·dag-processor 메모리가 한도 가까이, 깨어난 뒤 느림 | `scripts/stack.sh stats` | 90% 가 5분 넘게 이어지면 host_watch 가 warning. `docker restart urbanstream-airflow-scheduler` | R9 |
| Gold DAG 가 "이전 시도가 아직 실행 중입니다 (…)" 로 실패 | `python scripts/recover.py diagnose` 의 '작업 잠금' 줄 | 5절 | R10 |
| Discord 에 알림이 한동안 안 왔다 | `python scripts/host_watch.py --status` | 6절 | R12·R13 |
| 측정 표본 수집기·host_watch 가 Mac 재시작 뒤 꺼져 있다 | `python scripts/host_watch.py --start` | `python scripts/measure_sample.py --start` (STEP 2 동안) | R11 |

`recover.py` 는 비밀 값(.env, 웹훅 주소)을 출력하지 않고, `data/` 에서 아무것도 지우지 않으며, `stack.sh reset`·`down` 을 부르지 않는다.
바꾸는 명령(`resume`, `bronze --apply`)은 STEP 2 측정이나 훈련이 도는 동안(`data/measure/.lock` 이 있으면) 시작하지 않는다.

## 2. resume — 멈춘 스택을 순서대로 켜기

```zsh
python scripts/recover.py resume
```

하는 일: Docker 가 답하는지 확인 → 수집기를 뺀 나머지(`core,streaming,silver,airflow,monitor,api,dashboard`)를 `docker compose start` →
모두 healthy 가 될 때까지 최대 5분 기다림 → 수집기 시작 → `scripts/stack.sh check`. 수집기를 마지막에 켜는 이유는 Kafka 가 준비되기
전에 보낸 메시지가 전달 실패로 쌓이지 않게 하려는 것이다. 컨테이너를 다시 만들지 않는다(재시작 횟수·OOM 기록이 남는다, D-214).

5분 안에 healthy 가 안 되면 수집기를 켜지 않고 멈춘 뒤, 어느 컨테이너가 남았는지 보여 주고 `diagnose` 를 안내한다.
컨테이너를 지우거나 다시 만들기 **전에** `diagnose` 가 보여 주는 `docker inspect` 줄로 재시작 횟수·OOM 을 먼저 남긴다.

## 3. Bronze 체크포인트 복구 (OffsetOutOfRange, D-255)

Kafka 보존 기간(7일)을 넘겨 꺼져 있었으면 체크포인트가 이미 지워진 offset 을 가리킨다. 그 구간의 메시지는 어디에도 없다.
복구는 '잃은 구간을 잃었다고 확정하고 남아 있는 가장 이른 offset 부터 이어 읽게' 하는 것이다. 그래서 자동으로 하지 않는다(결정 4).

먼저 미리보기. 표마다 잃을 메시지 수를 보여 주고 아무것도 바꾸지 않는다.

```zsh
python scripts/recover.py bronze
```

고칠 표만 골라 적용한다(예: bus_arrival). 잃을 건수를 다시 보여 주고 `yes` 를 쳐야 진행한다.

```zsh
python scripts/recover.py bronze bus_arrival --apply
```

순서: `spark-bronze` 멈춤 → 멈췄는지 확인 → 체크포인트 고침(바꾸기 전 파일은 `data/_recovery/<시각>/` 에 백업) → 시작 →
3분 안에 healthy 와 새 Bronze 파일 확인. 확인이 실패해도 **자동으로 되돌리지 않는다**(되돌리면 다시 OffsetOutOfRange). 화면에 나온
`cp -p` 되돌리기 명령과 `diagnose` 를 본다. 체크포인트나 `_spark_metadata` 를 손으로 옮기지 않는다(D-258).

자세한 원리와 손으로 하는 방법은 `step4-bronze.md` §8.

## 4. DQ 차단 (버스 표 '미래 시각', 11-24)

증상: `bus_delay`·`incident_impact_finalize`·`weather_traffic_analysis`·`baseline_build` 의 `check_upstream` 이
"block_mart 규칙이 실패" 로 **재시도 없이** 바로 실패한다(STEP 3B 부터. DQ 결과는 05:00 `dq_report_daily` 에서만 바뀌어 재시도해도 같다).

1. 시계부터 본다. 60초 넘게 어긋나 있으면 화면의 안내(Docker Desktop 재시작 → `resume`)를 따른다.

```zsh
python scripts/recover.py clock
```

2. 데이터가 정말 미래 시각인지, 수집기 시계가 늦어서 그렇게 보이는지 확인한다(`dag-fail-1124` 문서 3절 ③ 질의). 아직 원인을
   확정하지 않았으므로 시계를 고치는 코드는 넣지 않았다(결정 10, D-299).
3. 시계 지연이 맞고 마트를 지금 다시 돌려야 하면(되돌릴 수 있는 조치):
   - `config/dq_rules.yaml` 의 `bus_position_future_event`·`bus_arrival_future_event` 줄에서 `action: block_mart` 를 `action: alert` 로 바꾼다.
   - `dq_report_daily` 를 수동으로 다시 돌린다. **yaml 만 바꾸고 DQ 를 다시 돌리지 않으면 이미 기록된 block_mart 결과가 계속 막는다.**

```zsh
docker exec urbanstream-airflow-scheduler airflow dags trigger dq_report_daily
```

   - `python scripts/recover.py diagnose` 의 'DQ 차단' 줄이 '없음' 이 되면, Airflow 화면에서 실패한 마트 실행을 Clear 한다.
   - 원인을 고친 뒤 `action: block_mart` 로 되돌린다. `margin_sec` 을 크게 늘리지 않는다(진짜 미래 시각도 가린다).

## 5. 작업 잠금 (고아 재시도, D-212)

Gold 스테이징·증분·창 처리와 `silver_reconcile` 은 PostgreSQL 세션 잠금을 잡고 돈다. Mac 이 잠들었다 깨서 Airflow 가 앞 시도를
'고아' 로 보고 새 시도를 띄워도, 앞 시도가 아직 돌고 있으면 새 시도는 최대 10분 기다린 뒤 실패한다(같은 작업이 겹쳐 돌지 않는다).
앞 시도가 죽으면 연결이 끊기면서 잠금은 저절로 풀린다. 그래서 보통은 할 일이 없다. 다음 실행이 정상으로 돈다.

`diagnose` 가 "…가 N분째 잠금을 잡고 있다 (pid …)" 를 보여 주고, 그 작업이 정말 멈춘 것(진행 로그가 없음)이 확실할 때만 그 줄에
나온 `SELECT pg_terminate_backend(pid);` 를 Postgres 에서 친다. 잠금 이름 `uslock:<작업>:<dag>#<시도>:pid…` 로 누가 잡았는지 보인다.

## 6. 알림이 오는지 (host_watch, D-126)

알림은 원래 `monitor` 컨테이너가 보낸다. Docker Desktop 이 멈추면 monitor 도 같이 멈춰 알림이 0건이었다(11-24~25).
`host_watch` 는 Docker 밖(Mac 의 Python)에서 1분마다 돌며 웹훅으로 직접 보낸다. 무엇을 보는지는 `scripts/host_watch.py` 머리말.

```zsh
python scripts/host_watch.py --start
```

```zsh
python scripts/host_watch.py --status
```

- 보내지 않고 판정만 보려면 `python scripts/host_watch.py --print`.
- 웹훅 자체가 되는지는 `scripts/recovery_drill.sh webhook-test` (Discord 에 시험 메시지 1건).
- 웹훅이 5xx 를 주면 알림은 'failed' 로 기록되고, 다음 전송이 성공할 때 "이전 전달 실패 N건" 이 붙는다.
- **Mac 이 잠들거나 꺼지면 host_watch 도 멈춘다.** 그때는 어디서도 알림이 오지 않는다(Mac 한 대의 한계, Step 12 에서 다룬다).
- Mac 을 재시작하거나 로그아웃하면 저절로 켜지지 않는다(launchd 를 쓰지 않는다, D-290). `mac-on-off.md` 2-D.

## 7. 훈련 (일부러 고장 내기)

수집을 끊는 훈련(`kafka-stop`, `docker-restart`)은 **STEP 2 무거운 측정이 끝난 뒤(12-16 이후 예상)** 에만 한다. 그 전에는
`check` 와 `webhook-test` 만 한다. 측정과 훈련은 같은 잠금을 써서 겹치지 않는다.

```zsh
python scripts/drill.py list
```

```zsh
python scripts/drill.py check
```

```zsh
python scripts/drill.py run monitor-stop
```

`run` 은 실행 전 검사를 통과해야 시작하고, 무엇을 멈추고 몇 분 뒤 되돌리는지 보여 준 뒤 `yes` 를 받는다. 최대 시간이 지나거나
Enter·Ctrl-C 를 누르면 바로 되돌리고, 5분 연속 모두 healthy 가 되면 끝난다. 결과는 `docs/recovery-drills.md` 표에 한 줄 붙는다.
중간에 끊겼으면 `python scripts/drill.py restore <훈련>`. 일부러 일으키지 않은 장애는 `python scripts/drill.py record …` 로 같은 표에 적는다.
