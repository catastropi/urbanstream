# ADR-0016: STEP 2 성능·규모 측정 (실제·합성 분리, 안전장치, 측정 도구)

- 상태: 채택 (2026-11-25). 설계안의 결정 1~10 을 권고안대로 승인받았다. 수집 시작일은 2026-11-25 로 정했다.
- 관련 코드: `config/measure.yaml`, `scripts/measure_sample.py`, `scripts/measure_guard.py`, `scripts/measure_env.py`, `scripts/synth_bronze.py`, `scripts/measure_run.py`, `scripts/measure_report.py`, `batch/stage_runner.py`(`kind=measure`), `docker-compose.yml`(`measure` 프로필, `init: true`). 절차: `docs/runbooks/measure.md`. 기록: DEFERRED T장(D-283~D-292).

## 1. 맥락

지금까지의 숫자(README "규모와 실측")는 데이터가 작을 때 나온 값이라 확장성의 근거가 되지 못했다. 더 큰 입력에서 어디가 먼저 버티지 못하는지 알아야 다음 개선(로더 구조 M-2, 작은 파일 병합 H-5)을 할지 정할 수 있다.

제약은 세 가지였다.

- 측정하는 동안에도 수집·스트리밍은 계속 돌아야 한다. 공공 API 는 실시간 값이라 놓친 구간을 채울 수 없다.
- Mac 한 대(Docker CPU 4개, VM 메모리 약 10.7 GiB)에 12개 컨테이너가 이미 돈다. 11-23 재처리 때 CPU 가 꽉 차 다른 컨테이너까지 unhealthy 가 됐다(D-282).
- 합성 데이터가 실제 데이터·운영 DB 에 섞이면 안 된다.

## 2. 결정

| # | 결정 | 이유 |
|---|---|---|
| ① | **측정 대상 M1~M8.** M1 스트리밍 배치 시간, M2 자원 사용, M3 저장량, M4 정기 배치 시간, M5 재처리 여러 날짜, M6 합성 1×·5×·10× 의 Silver 빌드·Gold 스테이징·적재, M7 원본 대 병합 사본의 읽기 시간, M8 API 응답. Kafka 최대 처리량, 외부 API 부하, 클라우드·다중 노드는 뺀다 | STEP 1 에서 남긴 질문(여러 날짜 재처리)과 개선 판단에 필요한 것만 잰다. 위험한 부하 시험은 하지 않는다 |
| ② | **합성 표시는 스키마를 바꾸지 않고 네 곳에.** 폴더(`data/measure/synthetic/`), manifest, Parquet 메타데이터 `urbanstream.synthetic`, `ingest_id` 접두 `syn-`(`raw_uri` 는 `synthetic://`). 날짜는 실제 데이터 시작(11-03)보다 앞인 2026-01-05 부터만 | 열을 더하면 Bronze 계약 버전이 바뀐다. 날짜를 떼어 두면 실수로 섞여도 실제 날짜 질의에 걸리지 않는다 |
| ③ | **합성 Gold 는 같은 Postgres 의 별도 DB `urbanstream_measure`, 끝나면 DROP.** 도구가 다른 DB 이름을 거부한다 | 컨테이너를 하나 더 띄우면 메모리가 더 든다. 운영 DB 안의 별도 스키마는 운영 표·함수와 섞일 위험이 있다 |
| ④ | **Spark 는 gold-runner 큐(`kind=measure`), Python·PG 는 `measure` 컨테이너(CPU 1, 1 GiB, 평소에는 뜨지 않음).** `kind=measure` 는 측정 폴더 안에서만 쓰고, `nice 10`, 한도 1200초 | Spark JVM 이 동시에 두 개 뜨지 않는 STEP 3A 구조를 그대로 쓴다. scheduler 는 메모리 사고 이력이 있다 |
| ⑤ | **합성 규모는 하루치 1×·5×·10×, 디스크 합계 8 GiB 이하** | 10× 면 로더 구조의 한계를 보기에 충분하다 [추정]. 그 이상은 이 Mac 에 부담이 크다 [추정] |
| ⑥ | **반복: 예열 1회 제외, 5분 미만 5회, 5~30분 3회. 중앙값과 최소·최대. 비교는 ABAB 순서, 범위가 겹치면 "차이 불분명"** | 쓸 수 있는 창이 하루 몇 개뿐이다. 적은 횟수에서는 중앙값과 범위가 평균·표준편차보다 정직하다 |
| ⑦ | **개선 작업 조건.** M-2 는 10× 로더 메모리 512 MiB 초과 또는 5×→10× 시간비 2.5 초과일 때만, H-5 는 병합 사본이 읽기 시간을 30% 이상 줄일 때만 설계안을 따로 올린다 | 숫자를 위한 개발을 막는다 |
| ⑧ | **STEP 2 동안 Docker CPU 4개 고정** | 중간에 바꾸면 앞뒤 숫자를 비교할 수 없다 |
| ⑨ | **요약 CSV·환경 기록은 `docs/performance/` 에 커밋, 원시 표본은 `data/measure/`(git 제외)** | 표의 숫자를 저장소 안에서 다시 만들 수 있고 저장소가 커지지 않는다 |
| ⑩ | **도구와 5분 표본 수집은 바로, 무거운 측정은 수집 시작(11-25) 14일 뒤인 12-09 부터** | 지금 가장 귀한 것은 끊기지 않는 수집이다. M1·M2 는 그때 떠 두지 않으면 다시 만들 수 없다 |

### 안전장치

- **실행 전 검사 9가지**: 컨테이너 12개 정상, VM 메모리 여유 2.5 GiB 이상, 직전 1분 CPU 합 200% 미만, 다른 무거운 작업 없음(gold-runner 대기·실행, 진행 중 재처리), 금지 시간대 밖이고 다음 금지 시간대 전에 끝남, 디스크 여유 15 GiB 이상, 전원 연결과 caffeinate, 잠금 파일 없음, 최근 30분 새 수집 공백 없음. 모르는 값은 FAIL 로 본다.
- **금지 시간대(KST)**: 매시 :15~:40(`gold_intraday`), 00:45~05:30(일일 배치). 한 회 상한 25분.
- **실행 중 감시(10초)**: 대상이 아닌 컨테이너가 unhealthy·정지, CPU 340% 초과 60초, 메모리 여유 1 GiB 미만, Kafka·Spark 재시작, 표본 간격 30초 초과(잠듦) 중 하나면 그 회차를 취소한다. 연속 두 번 취소되면 멈춘다.

## 3. 적용하면서 고친 것 (11-25)

| 문제 | 고친 것 | 기록 |
|---|---|---|
| Mac 의 Python 이 3.11 인데 3.12 전용 f-string 을 썼다 | 3.11 문법으로 고치고 `ruff.toml` 대상 버전을 3.11 로 | D-283 |
| `docker stop` 이 좀비 프로세스 때문에 실패 | 우리 코드가 PID 1 인 서비스 9개에 `init: true` | D-289 |
| launchd 표본 수집기가 주기 실행되지 않음(원인 미확정) | 수집기가 스스로 반복(`--start`/`--status`/`--stop`) | D-290 |
| 프로필 하나만 켠 `docker compose` 명령이 "no such service" | 문서·`measure_run.py` 의 명령에 `core` 를 함께 켬. 문서의 모든 프로필 조합을 시험으로 검사 | D-291 |
| 수집기 컨테이너화 뒤 끄기·켜기 절차가 바뀜 | `mac-on-off.md` 를 `docker stop` / `docker compose start` 기준으로 다시 씀 | D-292 |

## 4. 결과

- 합성 1배 하루치(64,650행)로 생성 → `kind=measure` Silver 빌드 → 포인터 → Gold 스테이징 → 측정 DB 적재를 시험 환경에서 끝까지 돌렸다(`tests/spark/test_synth_bronze.py`). Mac 실측 숫자는 12-09 이후 `docs/performance.md` 에 들어간다.
- Mac 에서 확인하지 못한 것: cgroup `memory.peak`(D-284), Docker Desktop 설정 파일(D-286), `init: true` 로 다시 만든 컨테이너의 정지(D-289), 표본 수집기의 하루 연속 동작(D-290).
