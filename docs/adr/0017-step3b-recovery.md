# ADR-0017: STEP 3B 복구 자동화와 장애 훈련

- 상태: 채택 (2026-11-26). 설계안 13장의 결정 1~12 를 권고안(A)대로 승인받았다. 승인 조건은 "현재 코드·버전과 맞으면" 이었고,
  맞지 않은 4곳은 아래 3장처럼 설계를 코드에 맞춰 바꿨다.
- 관련 코드: `config/recovery.yaml`, `scripts/recover.py`, `scripts/host_watch.py`, `scripts/drill.py`, `scripts/hostlib.py`,
  `batch/jobs.py`(작업 잠금), `batch/dq_jobs.py`(DQ 차단 즉시 실패, silver_verify 실행 위치), `batch/stage_runner.py`(`kind=silver_verify`).
  절차: `docs/runbooks/recovery.md`. 기록: `docs/recovery-drills.md`, DEFERRED U장(D-293~D-303).

## 1. 맥락

11-16~11-25 사이 실제로 생긴 장애(Kafka 보존 초과, Airflow 폭주, Kafka unhealthy, 버스 표 DQ 차단, Docker Desktop 정지, 좀비, launchd)는
모두 사람이 로그를 읽고 그때그때 명령을 골라 풀었다. 가장 큰 장애(11-24~25 Docker 정지)에서는 알림이 0건이었다. 알림을 보내는
monitor 컨테이너가 Docker 와 같이 멈췄기 때문이다(D-126). 고아 재시도가 같은 Gold 작업을 두 번 돌린 일(D-212)은 STEP 3A 의
gold-runner 로 Spark 는 하나만 뜨게 됐지만, 같은 작업의 DB 쓰기가 겹치는 것은 막지 못했다.

제약: 11-25 부터 수집 2주(STEP 2 측정의 바탕)가 돌고 있다. 그동안 일부러 수집을 끊으면 안 된다. 사용자는 Mac 한 대에서
명령을 복사해 붙이는 방식으로 운영한다.

## 2. 결정

| # | 결정 | 이유 |
|---|---|---|
| 1 | 수집을 끊는 Mac 훈련(kafka-stop, docker-restart)은 STEP 2 무거운 측정 뒤(12-16 이후 예상) | 측정을 해치지 않는다. 공백은 훈련으로 따로 센다 |
| 2 | 3B-1 을 12-08 이전에 한 번 적용 | 작업 잠금·silver_verify 이동·감시는 측정 기간의 실패를 줄인다 |
| 3 | 실제 Kafka 시험은 CI weekly(`recovery-kafka` 잡) | 매 push 는 가볍게 둔다 |
| 4 | Bronze 체크포인트 복구는 미리보기 + `--apply` + `yes` | 데이터를 잃었다고 확정하는 조치라 사람이 본다 |
| 5 | Docker·monitor 가 멈췄을 때 알림은 host_watch(호스트 Python, 외부 계정 없음) | 가장 큰 빈칸을 계정 없이 메운다. Mac 이 꺼진 경우는 여전히 못 잡는다 |
| 6 | Docker Desktop 재시작은 런북의 두 명령을 사람이 친다 | 다른 앱·VM 전체에 영향이 크다 |
| 7 | 재시작 정책 `on-failure` 유지 + `recover.py resume` | 켜는 순서(Kafka 먼저, 수집기 마지막)를 지킨다 |
| 8 | `recovery_drill.sh` 는 `drill.py` 를 부르는 껍데기로 남긴다 | 예전 런북의 명령이 그대로 된다 |
| 9 | raw → Kafka 재발행 도구는 만들지 않는다(후보로 기록, D-301) | 필요했던 적이 없다. 공백은 잃은 건수로 남긴다 |
| 10 | 수집기 시계 보정은 원인 확인(dag-fail-1124 ③) 뒤에. 시계 감시는 지금 넣는다 | 원인을 확인한 뒤 고친다. ③ 결과가 아직 없어 이번에는 감시만(D-299) |
| 11 | 고아 재시도 잠금은 PostgreSQL 세션 잠금(`pg_try_advisory_lock(3217, crc32(이름))`) | 모든 실행 위치가 이미 DB 에 붙는다. 프로세스가 죽으면 연결과 함께 저절로 풀린다 |
| 12 | 훈련 기록은 `docs/recovery-drills.md`(커밋) + `data/drills/`(원시) | 완료 판정이 '기록이 있음' 이다 |

## 3. 설계안과 달라진 것 (호환성 검증, 11-26)

| 설계안 | 실제 코드 | 바꾼 것 |
|---|---|---|
| host_watch 가 monitor 의 박동 파일을 본다 | 박동 파일은 monitor 컨테이너 안 `/tmp` 에 있어 호스트에서 보이지 않는다 | `ops.monitor_state.heartbeat_at`(DB) + 컨테이너 상태로 본다 |
| `check_upstream` 재시도 3→1 | `check_upstream` 은 DQ 차단 확인과 적재 확인 두 가지를 한다. 적재 확인은 기다리면 풀린다 | DQ 차단만 `AirflowFailException` 으로 재시도 없이 실패, 적재 확인은 재시도 3회 그대로 |
| 수집기·scheduler 컨테이너 시계를 각각 비교 | 컨테이너는 모두 하나의 Docker VM 시계를 쓴다 | Mac 시계와 VM 시계 하나만 비교한다(두 컨테이너 값 차이는 측정 오차로 표시) |
| (없음) | `tests/spark/test_synth_bronze.py` 와 `tests/unit/test_synth_bronze.py` 의 파일 이름이 같아 `pytest` 전체 수집이 오류로 멈춘다 | Spark 쪽을 `test_synth_bronze_spark.py` 로 바꿨다(D-298) |

설계안은 수집기를 다시 빌드한다고 했지만, 시계 보정을 넣지 않았으므로 수집기는 바뀌지 않는다. 적용할 때 수집 공백이 없다.
compose 도 바뀌지 않는다. gold-runner 는 코드를 마운트하므로 `docker restart` 만 한다.

## 4. 시나리오와 시험

| # | 시나리오 | 감지 | 복구 | 자동 시험 |
|---|---|---|---|---|
| R1 | Kafka 보존 초과로 Bronze OffsetOutOfRange (D-255) | diagnose, host_watch | `recover.py bronze` | `tests/spark/test_bronze_recover_kafka.py`(명령 단위 포함), `tests/recovery/test_recover_cli.py` |
| R2 | spark-bronze 가 배치 도중 kill -9 | host_watch | 자동 재시작 / `docker start` | `tests/spark/test_crash_restart_kafka.py` |
| R3 | spark-silver 가 멈췄다 다시 뜸 | 레이어 정체 알림 | `docker start` | `tests/spark/test_silver_restart.py` |
| R4 | Kafka 브로커 kill -9 → 재시작 | monitor critical | `docker start` | `tests/spark/test_crash_restart_kafka.py`, `tests/recovery/test_publisher_kafka.py`, `tests/unit/test_publisher_outage.py` |
| R5 | Kafka unhealthy (D-281) | host_watch 5분 | 원인 제거 | `tests/unit/test_compose_policy.py::test_kafka_healthcheck_timeout_wraps_the_jvm_itself` |
| R6 | Docker Desktop 정지·고착 | host_watch(`docker version` 3회) | 런북 두 명령 → `resume` | `tests/recovery/test_recover_cli.py`, `test_host_watch.py` (가짜 docker) |
| R7 | 좀비로 `docker stop` 실패 (D-289) | diagnose 'init' | mac-on-off 1-A | `test_recover_cli.py` |
| R8 | 컨테이너 시계 지연 → 버스 표 DQ 차단 | host_watch 시계(5분마다), diagnose 'DQ 차단' | 런북 4절 | `test_host_watch.py`, `tests/unit/test_job_lock.py`(즉시 실패) |
| R9 | Airflow 폭주 (D-280) | host_watch 메모리 90%·PIDS 300 이 5분 | `docker restart` | `test_host_watch.py` |
| R10 | 고아 재시도가 같은 작업을 겹쳐 돌림 (D-212) | 잠금 대기 로그, diagnose '작업 잠금' | 저절로 풀림 | `tests/unit/test_job_lock.py`, `tests/integration/test_job_lock_pg.py` |
| R11 | launchd 가 주기 실행 안 함 (D-290) | `stack.sh check` 의 host_watch 줄 | `--start` | `test_host_watch.py` (스스로 반복·PID 파일) |
| R12 | monitor 가 멈춤 (D-126) | host_watch 박동 10분 | `docker start` | `test_host_watch.py` (깨어난 직후 유예 포함) |
| R13 | 웹훅 5xx | 다음 성공에 "이전 전달 실패 N건" | — | `tests/integration/test_notify_pg.py` |
| R14 | Mac 이 꺼짐·잠듦 | 감지 못 함 | — | 시험하지 않음(한계) |

시험하지 않은 것: Docker Desktop 자체, Mac 절전, VM 시계의 실제 지연. 판단 로직만 가짜 `docker` 로 시험하고 실제 동작은
훈련과 자연 발생 기록으로 남긴다. 실제 Kafka 시험은 시험 전용 KRaft 브로커(운영과 같은 3.9.2)를 시험이 직접 띄우고 죽인다.

## 5. 결과

- 좋아진 것: 증상마다 칠 명령이 하나로 정해졌다(`diagnose`). Docker 가 멈춰도 알림이 온다(Mac 이 깨어 있는 한). 같은 Gold 작업이 겹쳐 돌지 않는다.
  scheduler 안에서 돌던 마지막 Spark(silver_verify)가 gold-runner 로 옮겨졌다. DQ 차단은 20분 간격 3번(약 1시간) 재시도하지 않고 바로 실패해 알린다.
- 대가: host_watch 는 Mac 재시작 뒤 사람이 다시 켜야 한다. Gold 작업이 DB 연결을 하나 더 쓴다(잠금 유지용, 작업당 1개).
- 되돌리기: `apply_step3b.sh --rollback`(적용 전 파일로). 잠금만 끄는 스위치는 두지 않았다. 문제가 생기면 롤백한다.
