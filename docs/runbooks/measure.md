# 성능·규모 측정 런북 (STEP 2)

Mac 에서 측정을 돌리는 순서. 도구는 `scripts/measure_*.py`, `scripts/synth_bronze.py`, 기준 값은 `config/measure.yaml`.
결과 표는 `docs/performance.md`(측정 뒤 `scripts/measure_report.py` 가 만든다).
모든 명령은 프로젝트 폴더에서, conda `urbanstream` 환경으로 실행한다.

## 0. 먼저 알아둘 것

- **장기 연속 수집 시작일은 2026-11-25** 이다(`collection_start`). 스트리밍 배치 시간·자원 사용·저장량·정기 배치 시간(M1~M4)은 이날부터 센다.
- **무거운 측정(재처리 7일, 합성 1×·5×·10×, 작은 파일 비교)은 2026-12-09 이후**에 한다(`heavy_after_days: 14`). 그 전에는 5분 표본 수집기만 켜 둔다. 중간에 수집이 길게 끊기면 그만큼 뒤로 민다(`collection_start` 를 고치고 커밋).
- **STEP 2 동안 Docker Desktop CPU 는 4개로 둔다.** 중간에 바꾸면 앞뒤 숫자를 비교할 수 없다.
- 측정 도구가 쓰는 곳은 `data/measure/` 뿐이다(git 제외). 운영 폴더(`data/bronze` 등)·운영 DB 에 쓰려 하면 시작하지 않는다.
  예외는 gold-runner 요청 파일 하나(`data/gold_stage/_requests/measure-*.json`)다. 실행기가 이 파일을 보고 측정 작업을 돌린다.
- 측정 금지 시간대(KST): **매시 :15~:40**, **00:45~05:30**. 쓸 수 있는 창은 매시 :40 ~ 다음 시 :15 의 35분이다. 지금이 어떤지:

```zsh
python scripts/measure_guard.py window
```

## 1. 적용 직후 한 번 (도구 ZIP 적용 뒤)

1. gold-runner 를 다시 만든다(측정 폴더 마운트가 생긴다). 다른 컨테이너는 그대로 둔다.

```zsh
mkdir -p data/measure/samples && chmod -R a+rwX data/measure
```

```zsh
COMPOSE_PROFILES="core,airflow" docker compose up -d --no-deps gold-runner
```

2. 환경 기록 한 번. `data/measure/env/env-<시각>.json` 이 생긴다. 비밀 값은 적지 않는다(한도 변수 이름과 값만).

```zsh
python scripts/measure_env.py --note "STEP 2 시작"
```

3. 표본을 한 번 떠서 화면으로 본다(파일에 쓰지 않음). 컨테이너 12개, `batches` 에 Bronze·Silver 배치가 보이면 정상이다.

```zsh
python scripts/measure_sample.py --print | head -40
```

## 2. 5분 표본 수집기

M1(스트리밍 배치 시간)과 M2(자원 사용)는 그때 떠 두지 않으면 다시 만들 수 없다. Spark 는 체크포인트의 배치 기록을 최근 100개만 남긴다.
수집기는 스스로 5분마다 반복하는 백그라운드 프로세스다. 터미널을 닫아도 계속 돈다. 하나만 돈다(`data/measure/samples/sampler.pid`).

1. 켠다. 바로 한 번 뜨고, 그 뒤 5분 배수 시각(:00, :05, …)마다 뜬다. macOS 에서는 `caffeinate -i` 아래에서 돌아, 가만히 둬서 Mac 이 자는 것도 막는다(덮개를 닫아 자는 것은 못 막는다).

```zsh
python scripts/measure_sample.py --start
```

2. 확인. `돌고 있음` 이고 마지막 표본이 10분 안이면 된다. 끝 코드는 0 = 정상, 1 = 멈춤, 2 = 돌지만 표본이 늦음.

```zsh
python scripts/measure_sample.py --status
```

회차마다 한 줄씩 남는 기록과 오류:

```zsh
tail -5 data/measure/samples/sampler.log
```

3. 끄기:

```zsh
python scripts/measure_sample.py --stop
```

- **Mac 을 재시작하거나 로그아웃하면 저절로 다시 켜지지 않는다.** 켤 때(`mac-on-off.md` 2절 끝) `--start` 를 다시 한다. 이미 돌고 있으면 시작하지 않고 알려 준다.
- Mac 이 자는 동안은 표본이 없다. 깨어나면 밀린 회차를 채우지 않고 다음 5분 배수에 뜬다. 그 구간은 표본 시각 사이의 빈 곳으로 보이고, 보고서가 날짜별 '온전한 날'(공백 합 30분 미만)을 따로 센다.
- Docker 가 꺼져 있는 동안에도 돈다. 그 회차는 `"docker": false` 로 남고 보고서는 그 구간을 빈 구간으로 센다.
- 한 회차가 실패해도(예외) 기록에 `표본 실패` 를 남기고 다음 회차는 돈다.
- 하루 표본 파일은 1 MB 안팎이다 [추정].
- **launchd 는 쓰지 않는다.** 11-25 이 Mac 에서 `launchctl load` 한 작업이 주기 실행되지 않았다. `launchctl print` 의 `runs = 0`, 시스템 로그에는 5분마다 `pending spawn, domain in on-demand-only mode` 가 남았다. `kickstart` 로 손으로 띄우면 기록됐다. 원인은 확정하지 못했다 [추정: 로그인 세션의 launchd 도메인이 요청이 있을 때만 띄우는 상태였다]. 예시 파일(`scripts/launchd/`)은 다른 Mac 용으로만 남긴다(D-290). 이미 등록했으면 지운다(두 방식이 같이 돌면 커서 파일을 서로 덮어쓴다):

```zsh
launchctl bootout gui/$(id -u)/com.urbanstream.measure-sample; rm -f ~/Library/LaunchAgents/com.urbanstream.measure-sample.plist
```

## 3. 안전장치 시험 (한 번, 허용 창 하나)

1. 측정하는 동안에는 터미널 하나에서 잠자기를 막아 둔다. 전원 어댑터를 연결한다.

```zsh
caffeinate -i
```

2. 실행 전 검사. 모두 PASS 여야 능동 측정이 시작된다(약 1분 걸린다: CPU 를 네 번 떠서 평균).

```zsh
python scripts/measure_guard.py check
```

| 검사 | 기준 |
|---|---|
| 컨테이너 | 12개 모두 running, healthcheck 가 있는 것은 healthy |
| 메모리 여유 | Docker VM 메모리 − 컨테이너 실제 사용 합 ≥ 2.5 GiB |
| CPU | 직전 1분 컨테이너 CPU 합 평균 < 200% |
| 다른 무거운 작업 | gold-runner 대기·실행 0, `ops.reprocess_run` 진행 중 0 |
| 시간대 | 금지 시간대가 아니고 예상 소요가 다음 금지 시간대 전에 끝남 |
| 디스크 | 여유 ≥ 15 GiB 이고 예상 출력의 3배 이상 |
| 전원 | 전원 연결, caffeinate 실행 중 |
| 중복 실행 | `data/measure/.lock` 없음 |
| 수집 | 최근 30분 `ops.data_gap` 새 공백 0 |

3. 자동 중단 시험. 작은 합성(도로 소통 하루치)으로 Silver 빌드를 요청하고 20초 뒤 일부러 중단한다. `PASS 강제 중단` 이 나오면 감시·취소가 실제로 동작한 것이다. 합성 폴더는 끝나면 지운다.

```zsh
python scripts/measure_run.py selftest --profile tests/fixtures/measure/profile_small.json
```

## 4. 무거운 측정 (2026-12-09 이후, 하루 창 서너 개)

한 번에 하나만 한다(잠금 파일). 각 명령은 매 회 앞에서 2절의 검사를 다시 하고, 통과하지 못하면 거기서 멈춘다. 다음 창에서 같은 명령을 다시 실행하면 된다(이미 잰 회는 결과 파일에 남아 있다).
실행 중에는 10초마다 감시하고, 아래 중 하나면 그 회를 끊고 무효로 적는다: 측정 대상이 아닌 컨테이너 unhealthy, CPU 합 340% 초과가 60초, VM 메모리 여유 1 GiB 미만, spark-bronze·spark-silver·kafka 재시작, 감시 표본 사이 30초 넘는 공백(잠듦).
중단되면 모든 컨테이너가 5분 연속 정상일 때까지 기다린다. 같은 항목이 두 번 연속 중단되면 그 항목을 멈춘다(규모를 낮추거나 한계로 적는다).

반복 규칙: 첫 회가 5분 미만이면 첫 회는 예열(통계에서 뺌) + 5회, 5~30분이면 3회. 30분 넘는 한 번짜리는 하지 않는다.

### 4-1. 수동 지표 집계 (M1~M4)

```zsh
python scripts/measure_report.py --storage
```

### 4-2. 재처리 하루씩 7개 날짜 (M5)

재처리는 지금처럼 `reprocess` DAG 로 한다(절차는 `docs/runbooks/reprocess.md`). 날짜 하나, 허용 창 하나. 표 그룹 두 개(`silver_weather,silver_traffic_speed` / 나머지 세 표)로 나눈다.
DAG 를 켜기 직전에 다른 터미널에서 감시를 켠다. 감시가 걸리면 실행 중인 gold-runner 요청을 취소한다(재처리는 실패로 끝나고 게시되지 않는다).

```zsh
python scripts/measure_run.py watch --label m5-2026-11-27-a
```

소요 시간은 `ops.reprocess_run`·`_build.json` 에 이미 남는다. 감시 기록(최대 CPU, 최소 메모리 여유, gold-runner 최대 메모리)은 `data/measure/results/watch.jsonl`.

### 4-3. 작은 파일 비교 (M7)

1. 2일 이상 지난 하루를 골라 사본을 만든다(표·시간당 파일 1개로 합침. 원본은 읽기만). 돌발은 파티션 규칙이 달라 뺀다.

```zsh
python scripts/measure_run.py copy --date 2026-12-07 --run-id c1207
```

2. 원본과 사본을 A, B, A, B 로 번갈아 잰다. 재처리 빌드(3쌍), `silver_verify` 읽기(5쌍).

```zsh
python scripts/measure_run.py m7 --date 2026-12-07 --run-id c1207 --what build
```

```zsh
python scripts/measure_run.py m7 --date 2026-12-07 --run-id c1207 --what verify
```

3. 끝나면 사본을 지운다.

```zsh
rm -rf data/measure/copy/c1207
```

판정: 사본의 최댓값이 원본의 최솟값보다 작고 중앙값이 30% 이상 줄었을 때만 compaction 설계안을 따로 올린다.

### 4-4. 합성 1×·5×·10× (M6)

1. 실제 하루 프로파일(기본 11-04. 읽기만 한다).

```zsh
python scripts/synth_bronze.py profile --date 2026-11-04
```

2. 합성 Bronze. 날짜는 2026-01-05 부터(실제 데이터와 겹치지 않는다). 규모마다 run-id 를 다르게 한다.

```zsh
python scripts/synth_bronze.py generate --profile data/measure/profile/2026-11-04.json --scale 1 --date 2026-01-05 --run-id s1
```

3. Silver 빌드 → 측정 폴더 안의 포인터 게시 → Gold 스테이징.

```zsh
python scripts/measure_run.py synth-silver --run-id s1
```

```zsh
python scripts/measure_run.py synth-point --run-id s1 --version m0
```

```zsh
python scripts/measure_run.py synth-stage --run-id s1
```

4. 측정 DB 를 만들고(같은 Postgres 컨테이너 안의 `urbanstream_measure`, 마이그레이션 001~ 적용) 적재를 잰다. 적재는 measure 컨테이너(CPU 1, 메모리 1 GiB)에서 돈다. 매 회 앞에 측정 DB 의 사실 표를 비운다.

```zsh
python scripts/measure_run.py db-create
```

```zsh
python scripts/measure_run.py synth-load --run-id s1
```

5. 5×, 10× 도 2~4를 같은 순서로(`--scale 5 --run-id s5`, `--scale 10 --run-id s10`). 규모 하나가 끝나면 그 합성 폴더를 지운다. 합계 8 GiB 를 넘기지 않는다.

```zsh
rm -rf data/measure/synthetic/s1
```

6. 모두 끝나면 측정 DB 를 지운다.

```zsh
python scripts/measure_run.py db-drop
```

판정: 10× 로더 최대 메모리 512 MiB 초과 또는 5×→10× 적재 시간비 2.5 초과일 때만 배치 COPY 로 바꾼다.

### 4-5. API 응답 (M8)

```zsh
python scripts/measure_run.py api --n 20
```

### 4-6. 결과 문서

```zsh
python scripts/measure_report.py --storage
```

`docs/performance.md`, `docs/performance/*.csv` 와 쓴 환경 JSON(`python scripts/measure_env.py --copy-to-docs`)을 커밋한다. 원시 기록(`data/measure/`)은 커밋하지 않는다.

## 5. 측정한 날 끝에 확인

- 수집 공백: Airflow UI 또는 `ops.v_data_gap_recent`. 측정 시간대에 새 공백이 있으면 STEP 2 능동 측정을 멈추고 원인부터 본다.
- 합성이 운영에 섞이지 않았는지. 둘 다 0 이어야 한다.

```zsh
find data/bronze data/silver data/gold_stage -name '*-syn.parquet' -o -name '*-copy.parquet' | wc -l
```

```zsh
docker exec urbanstream-postgres psql -U urbanstream -d urbanstream -tAc "SELECT (SELECT count(*) FROM fact.traffic_speed WHERE dedup_key LIKE 'S%-%') + (SELECT count(*) FROM ops.call_audit WHERE ingest_id LIKE 'syn-%')"
```

## 6. 멈추거나 되돌릴 때

- 측정 하나를 당장 멈추기: 그 터미널에서 Ctrl+C 한 뒤, 실행 중인 측정 요청을 취소한다(실행기가 5초 안에 Spark 를 끈다).

```zsh
for f in data/gold_stage/_requests/measure-*.running; do [ -e "$f" ] && touch "${f%.running}.cancel"; done
```

- 측정 컨테이너가 남아 있으면: `docker ps --filter name=urbanstream-measure` 로 보고 `docker kill <이름>`.
- 잠금 파일이 남아 다음 측정이 시작되지 않으면(비정상 종료 뒤): 다른 측정이 없는지 보고 `rm data/measure/.lock`.
- 표본 수집기 끄기: `python scripts/measure_sample.py --stop` (2절).
- 측정 데이터 전부 지우기(표본까지 사라진다. 결과를 커밋한 뒤에만): `rm -rf data/measure`, `python scripts/measure_run.py db-drop`.
