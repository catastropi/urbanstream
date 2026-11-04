# 미완료·미확인 장부 (DEFERRED)

"아직 안 한 것 / 확인 못 한 것 / 일부러 미룬 것"을 **어디에 추가할지**와 함께 적는 단 하나의 목록이다.
새 채팅은 HANDOFF.md 와 함께 이 파일을 읽는다. 항목을 끝내면 상태를 바꾸고 날짜를 적는다 (지우지 않는다 — 이력이 곧 근거다).
마지막 갱신: 2026-11-04 (Step 6 Gold 구현 — 샌드박스 PostgreSQL 16 에서만 검증, Mac PG17 실행 대기).

**상태:** ⏳ 미착수(할 일) · ❓ 확인 못 함(사실 여부 모름) · ⏸ 일부러 보류(이유와 재검토 시점 있음) · 👤 사용자 작업 · ✅ 완료(날짜)
**출처:** H9 = HANDOFF 9장, A6 = ADR-0006, 11-03 = 이번 대화에서 발견

---

## A. Step 5 남은 작업 (이 순서로)

| ID | 항목 | 상태 | 현재 알려진 것 | 어디에 추가할지 | 완료 기준 / 확인 방법 |
|---|---|---|---|---|---|
| D-00 | **5-0 Bronze 프로파일링 실행** | ✅ 2026-11-04 | Mac(Java 17 컨테이너)에서 실행 성공, 컨테이너 테스트 `21 passed, 57 deselected`(= 전체 78). 표본은 3.5시간+12분뿐 | `docs/runbooks/step5-silver.md` | 평일 퇴근 시간대 포함 24시간 뒤 재실행(D-01 재확인) |
| D-01 | 워터마크 값 | ✅ 잠정 확정 2026-11-04 (ADR-0007 2-1) | traffic·position·arrival `event_time` 10분 / incident `fetched_at` 10분 / weather `event_time` 3시간. 버스위치 최대 늦음 206초, 나머지 0. **꼬리는 미확인** | `config/silver.yaml` (5-1 에서 생성) | 24시간치 후 `lateness_sec` p99·max 가 표 값의 1/3 안인지 재확인 |
| D-02 | 0값/결측 정책 | ✅ 규칙 확정 2026-11-04 (ADR-0007 2-2) | 표본에 0·결측 **0건**이라 데이터 근거는 없음. 보존+`dq_flags` 표시만 | Silver 변환 코드, `config/silver.yaml` | 비 오는 날·정체 시간대 재프로파일(D-41) |
| D-03 | 돌발 `is_long_term` 기준 | ✅ 2026-11-04: `planned_duration_days` 저장 + 7일 초과 (ADR-0007 2-3) | 분포가 ≤1일 92 / >30일 2 / 사이 0 → 데이터로는 7 을 고를 근거 없음, **판단**임을 ADR 에 명시 | `config/silver.yaml`, Silver 돌발 표 | 며칠 쌓인 뒤 1~7일·7~30일 구간 확인 |
| D-04 | Silver 표 5개 구현 | ✅ 2026-11-04 (Mac `silver_verify` 통과) | `streaming/silver*.py`, `config/silver.yaml`. 컨테이너 테스트 105 passed. 5개 표 유실 0·초과 0 | `docs/runbooks/step5-silver.md` | — |
| D-05 | 돌발 Silver = 관측 로그 + `observed_date` 분할 | ✅ 2026-11-04 | Mac: 돌발 중복 0 (Bronze 1,002행 기준) | — | — |
| D-06 | 좌표 변환 (돌발 `tm_x/y` EPSG:5181 → WGS84) | ✅ 2026-11-04 | pyproj **대신** 표준 라이브러리 구현(`streaming/geo_tm.py`) → 이미지에 의존성 추가 없음. pyproj 3.8 과 2,000점 비교 오차 2.4e-13도, 실제 버스 GPS↔TM 샘플과 10 m 안에서 일치(테스트) | `streaming/geo_tm.py`, `tests/unit/test_geo_tm.py` | 돌발 Silver 의 `lon/lat` 이 설명문 지명과 맞는지 눈으로 몇 건 확인은 ⏳ (D-46) |
| D-07 | `spark-silver` 컨테이너 | 가동 ✅ 2026-11-04, **실측 ⏳** | 기동·검증 통과. Silver 실제 메모리·dedup 상태 크기, 4개 컨테이너 합계가 Docker VM(7.85 GiB) 안에 드는지는 **미측정** | `docker-compose.yml` | `scripts/stack.sh stats` 로 실측해 ADR 실측표에 기록 |
| D-08 | Bronze→Silver 행 수 등식 DQ | ✅ 2026-11-04 (Mac 통과) | 한계: 비교 기준이 Silver 마지막 `fetched_at` −15분이라 데이터가 끊기면 마지막 15분은 비교 밖. "수집 = 정상 토픽 + dlq" 는 아직 이 스크립트에 **없음** | Step 8 DQ | dlq 포함 등식은 Step 8 |
| D-09 | 수집 공백 표시 | ⏳ (원인은 ✅) | 공백 2회, 둘 다 Mac 덮개 닫힘 잠자기와 시각이 일치(`pmset -g log`, 11-04): ① 16:22:58 Clamshell Sleep → 17:59:55 Wake (공백 16:26~18:00) ② 20:12:53 Clamshell Sleep(버스 마지막 수집 20:12:22) → 00:20:59 Wake(중간 DarkWake 때만 일부 수집). Kafka·raw 어디에도 없어 **복구 불가** | Silver 메타 또는 Step 8 DQ 신선도 규칙 (`config/dq_rules.yaml`) | `fetched_at` 간격으로 공백 구간 목록 산출 |
| D-10 | ADR-0007 작성 | ✅ 2026-11-04 | `docs/adr/0007-silver-rules.md` (열 설계·구현은 5-1~5-4 에서 이 ADR 에 덧붙이지 않고 코드·테스트·런북으로) | — | — |
| D-11 | Step 5 완료 후 HANDOFF·zip 갱신 | ⏳ | 사용자 요청: 첫 메시지와 같은 방식 (HANDOFF.md + `urbanstream_full.zip`, `.env`·`data/` 제외) | 맨 마지막 | 새 폴더에 풀어 pytest, 키 스캔 |

## B. 일부러 미룬 결정 (재검토 시점 있음)

| ID | 항목 | 상태 | 이유 | 재검토 시점 / 신호 | 어디에 추가할지 |
|---|---|---|---|---|---|
| D-20 | **Delta Lake 도입** | ⏸ | A6 결정 2. 샌드박스에서 Maven Central 접속 불가 → **Spark 4.1.3 + Delta 실제 동작 미확인**. 웹 확인: Delta 4.1~4.4.1 은 Spark 4.1.0 용 `delta-spark_4.1_2.13` 제공, Java 17+ | 신호 3개: 스트리밍이 쓴 표를 제자리 압축해야 할 때 / Spark 안 MERGE 필요 / `_spark_metadata` compact 로그가 시작 시간을 늘릴 때 | Dockerfile(jar 굽기 — `--packages` 금지), ADR |
| D-21 | **폴더 규칙** "스트리밍이 쓰는 폴더는 배치가 안 건드린다" | 일부 ✅ 2026-11-04 | Silver 컨테이너는 Bronze 를 `:ro` 로 마운트(compose 테스트). 샌드박스 4.1.3: 로그에 없는 파일은 배치·스트리밍 모두 못 봄, 로그에 있는 파일을 지우면 배치는 오류·스트리밍은 **조용히 누락**. Step 7 설계 반영은 ⏳ | Step 7 `retention_cleanup`, 압축, `backfill_from_raw` 설계. 위반 방지 테스트 | `batch/`, `tests/spark/` |
| D-22 | `replay_raw.py` 대신 `backfill_from_raw` | ⏸ | A6 결정 3. 11-03 기준 raw 전용 구간은 약 15:06~16:13 한 시간뿐. 11-02 raw 폴더 없음 | Step 7. 과거 구간이 필요한 쪽은 문제 A 기준선(몇 주) — 어차피 라이브 누적이 지배적 | `batch/`, `dags/`(백필 DAG) |
| D-23 | Bronze 파일 압축 | ⏸ | 하루 약 8,300개(52분 관측 외삽, ADR-0005 의 18,720 은 상한 과대), 평균 20~60KB. 스트리밍 읽기는 폴더 목록이 아니라 로그를 따르므로 당장 병목 아님 | 로그 compact 크기 주 1회 확인(`ls -S data/bronze/*/_spark_metadata/*.compact`), 하루 약 2.1 MB 증가 추정 | Step 7, D-21 규칙 준수 |
| D-24 | Bronze 보관 정책 | ⏸ | 하루 약 0.15 GB 로 외삽(`du` 반올림 포함), raw 대비 약 6배. Mac 여유 33 GiB 당시 | 디스크 점검 시 / Step 7 | `batch/retention_cleanup`, 런북 |
| D-25 | ADR-0005 본문 정정 | ✅ 11-03 | "18,700 개/일" 과 "일 단위 압축은 Step 7 배치" 를 정정 1 로 기록 | — | `docs/adr/0005-bronze-spark.md` 끝 |

## C. 확인 못 한 것 (사실 여부 모름)

| ID | 항목 | 상태 | 확인 방법 | 어디에 기록할지 |
|---|---|---|---|---|
| D-30 | 스트리밍 파일 소스의 Mac 바인드 마운트 읽기 속도, Silver+Bronze 동시 CPU 경합 | ❓ | Silver 가동 후 Spark UI(Bronze http://127.0.0.1:4040, Silver http://127.0.0.1:4041)의 배치 시간, `stack.sh stats` | ADR 실측표 |
| D-31 | `docker compose run -v …:ro` 가 서비스의 rw 마운트를 덮어쓰는지 | ❓ | 지금 프로파일은 "코드가 안 쓴다"로만 보장(테스트 `test_profile_all_reads_but_never_writes_bronze`). Compose v2.10.2 동작 미확인 | `scripts/silver_profile.sh` 주석 |
| D-32 | `dlq` 에 실제 메시지가 들어왔을 때 Bronze 표 생성·검증 | ❓ (테스트로만 확인) | 일부러 계약 위반 응답을 만들어 흘리기 | Step 8 `tests/recovery/` |
| D-33 | 배치 도중 kill 반복(`docker kill` 여러 번, 타이밍 변경) 후 `bronze_verify` | ❓ | `docs/runbooks/step4-bronze.md` 5절 반복 | ADR-0005 검증 상태, Step 8 recovery |
| D-34 | 장시간 중단(3~5분 이상) 후 따라잡기 — `maxOffsetsPerTrigger` 가 나눠 읽는지 | ❓ | 컨테이너를 내렸다 켜고 로그·`bronze_verify` | ADR-0005 |
| D-35 | Bronze 메모리 장시간 추이 | ❓ | 610 MiB(9분)까지만 측정. 30분~하루 후 `stack.sh stats` | ADR-0005 실측표 |
| D-36 | 11-02 raw 가 왜 없나 (HANDOFF 는 11-02 12:24 부터 수집했다고 기록) | ❓ | 사용자가 폴더를 옮겼는지 기억/`data/` 아래 다른 위치 확인 (`find data -name '*.gz' -path '*11/02*'`) | 이 장부 |
| D-37 | 수집기 정지 원인 | ✅ 2026-11-04 | 덮개 닫힘 잠자기(D-09). 반복되므로 Step 8 알림 대상이자 상시 가동 방법 필요(D-44) | D-09 |
| D-38 | 돌발 "해제 시각 = 목록에서 사라진 첫 관측"인지, 같은 `acc_id` 내용 해시가 하루 동안 바뀌는 빈도 | ❓ | Silver 돌발 관측 로그가 하루 이상 쌓인 뒤 `exp_clr_*` 와 비교 | `docs/api-catalog/seoul_incident.md`, Gold(Step 6) |
| D-39 | 도로소통 `link_id` ↔ 돌발 `link_id` 조인 가능 여부 | ❓ | **도로소통은 링크 1개만 수집** 중이라 지금은 맞을 수가 없다. 링크 목록 확보 후 검증 | `docs/api-catalog/`, Step 6 `dim_link` |
| D-40 | 링크 지오메트리 + 수집 링크 목록 | ✅ 2026-11-04 (6-0) | LINK_VERTEX_2025(5,093링크, EPSG:5181)를 `dim.dim_link` 시드로 사용(`batch/seed_dim_link.py`, 파일은 git 제외). 수집 링크 4개로 확대(sources.yaml, 450초) / 이전: 후보 "소통 돌발 도로별 링크 정보" 데이터셋. 방법은 `docs/runbooks/step0-closeout.md` 4장 | `config/sources.yaml`(링크 목록), `sql/`(`dim_link`) — Step 6 전 필수 |
| D-41 | `RN1` 비 오는 날 응답 모양 | ❓ (H9) | 현재 `RN1` 원문은 `"0"` 6건뿐. Silver 는 `value_raw`+`value_num`+`value_unparsed` 로 대비. 비 오는 날 D-00 재실행 → `RN1_raw_values` | `docs/api-catalog/kma_ncst.md` |
| D-42 | 심야 버스 응답 모양(0대 응답 등) | ❓ (H9) | 버스는 04~24시만 수집. 23~24시대 D-00 재실행 | `docs/api-catalog/bus_position.md` |
| D-43 | 버스도착 `arrive_code=2` (99건/17,526)의 뜻 | ❓ | 원문 보존. Silver 생성 후 `arrive_code=2` 행의 `arrival_msg` 등 비교 | `docs/api-catalog/bus_arrival.md`, 계약 v2(D-57) |
| D-44 | 컨테이너 시계가 호스트보다 느렸던 구간(`ingested_at - fetched_at` 최솟값 -1,906초) | ❓ 가설 | 11-04 00:39 현재 호스트(15:36:27Z)와 컨테이너(15:39:32Z, 3분 뒤 측정)는 **일치**. 과거 구간의 어긋남 원인은 여전히 미확인(가설: 잠자기 뒤 Docker VM 시계 지연). Silver 는 `ingested_at<fetched_at` 행에 `ingest_clock_skew` 플래그만 단다. 수집기는 컨테이너가 아니라 호스트 프로세스(`docker ps` 에 없음). 상시 가동: AC 전원+덮개 열기, 또는 `sudo pmset -a disablesleep 1`(비공식·미검증, 끝나면 0) | `docs/runbooks/`, ADR-0005 |
| D-45 | Silver 시간 역전·미래 event_time 0 확인 | 코드 ✅ 2026-11-04, **Mac 실행 ⏳** | `silver_verify` 에 표마다 `미래 시각`·`시간 역전`(버스위치 차량별) 추가, 테스트·변이 시험 통과 | `streaming/silver_verify.py` | Mac 에서 1회 실행해 둘 다 0 |
| D-46 | 돌발 변환 좌표 육안 확인 | 코드 ✅ 2026-11-04, **Mac 확인 ⏳** | `silver_verify` 출력에 `[좌표 확인용]` 6줄(기간 긴 순) | `docs/api-catalog/seoul_incident.md` | 지명과 좌표가 맞는 건 5건 이상(표본이 6건뿐이면 가능한 만큼) |

## D. Step 3·0 에서 넘어온 선택 항목 (HANDOFF 9장)

| ID | 항목 | 상태 | 어디에 추가할지 |
|---|---|---|---|
| D-50 | 기상청 `resultCode 03`(NO_DATA)을 오류가 아닌 "발표 전" 상태로 분류 | ✅ 2026-11-04 (6-0) | KMA 03 및 버스위치 headerCd=4 를 `empty` 로 분류(실패 아님). 픽스처는 재구성본 / 이전: `src/urbanstream/contracts/weather.py`, `collector/runner.py`, 테스트 — Step 8 전 |
| D-51 | 서울 도로소통·돌발 호출 한도 확인(미확인, 보수적 1,000건) | 👤 | `config/sources.yaml`, ADR-0004 결정 7 |
| D-52 | 포털 호출 통계 vs `python -m collector status` 비교(상세기능 이름 `getBusPosByRtidList` vs `getBusPosByRtid`) | 👤 | ADR-0004 |
| D-53 | 도로소통 간격 120초를 퇴근 시간대 측정으로 재확인 | 👤 (선택) | `docs/api-catalog/seoul_traffic.md` |
| D-54 | 노선정보(15000193)·정류소정보(15000303) 승인 확인 후 `seoul-bus-ids` 재실행, 노선 범위 결정(노선 N개면 간격 N배) | 👤 | `config/sources.yaml` |
| D-55 | 새 코드 장시간 검증: audit 의 `attempts>=2`·`TotalTimeout` 확인 | ⏳ | Step 8 `health_stream` (그 전엔 `platform.audit` 토픽 grep, HANDOFF 3장 명령) |
| D-56 | 코드 바꾼 뒤 이미지 재빌드 잊지 않기 | 운영 규칙 | `docker compose --profile streaming build spark-bronze` (Bronze 재시작이 필요하면 `scripts/stack.sh up-streaming`) |
| D-57 | 의미 미확인 필드(버스위치 `lastStTm` 등, 버스도착 `exps/kals/neus`·`traSpd` 단위·미사용 컬럼 약 45개) → 계약 v2 | ⏳ | `src/urbanstream/contracts/`, `docs/api-catalog/` ("미확인" 칸을 측정값+날짜로 갱신) |

## E. 사용자 개인 작업

| ID | 항목 | 상태 | 비고 |
|---|---|---|---|
| D-60 | `git init` + 첫 커밋 (커밋 전 `git status` 에 `.env` 없는지 확인) | ✅ 2026-11-04 | 사용자 완료(비공개 레포 catastropi/urbanstream). 단 `.gitignore` 가 data/ 안쪽 파일을 막지 못하던 결함을 6단계에서 발견·수정 → D-71 확인 필요 / 이전: Step 5 코드가 늘기 전에 하길 권함 (되돌리기 쉬워짐) |
| D-61 | `SEOUL_API_KEY`·`DATA_GO_KR_KEY` 재발급 | 👤 확인 대기 | 사용자 3번 완료 보고, 스모크 통과. 재발급 키가 `.env` 에만 있는지 마지막 확인 / 이전: 11-02, 11-03 에 `.env` 가 든 zip 업로드된 적 있음. 이번 zip 에는 `.env` 없음(확인) |
| D-62 | `urbanstream` conda 환경(3.11)으로 실행하는 습관 | 👤 | 11-03 확인: 프롬프트에 `(urbanstream)` 표시됨 |
| D-63 | Tableau 사용 방식 결정(Desktop vs Public) | 👤 | Step 10 전 |

## F. Step 6 (Gold / PostgreSQL) — 11-04

| ID | 항목 | 상태 | 현재 알려진 것 | 확인 방법 / 어디에 |
|---|---|---|---|---|
| D-70 | fact 파티션 | ⏸ | 하루 수만 행이라 보류(ADR-0008 ④). 새 마이그레이션으로 추가 가능 | 행이 수천만 또는 보관 삭제가 필요해질 때 |
| D-71 | `.gitignore` 결함: `data/*` + `!data/*/` 때문에 data/ 안쪽 파일이 추적 대상이었다 (임시 저장소로 재현) | 👤 일부 | 규칙은 `data/**` 로 수정·테스트함(`tests/unit/test_gitignore.py`) Mac: data/ 추적 해제 커밋 완료(13:30). 단 **과거 커밋이 GitHub 에 이미 있음** → 공개 전에 `git filter-repo --path data --invert-paths` 후 force push 필요. | Mac: `git ls-files data` 가 비어야 함. 나오면 `git rm -r --cached data` 후 커밋, **이미 push 했다면 레포가 비공개여도 이력에 남음** → 알려 주세요 |
| D-72 | Mac(PG17.7/PostGIS 3.6.1)에서 `migrate.py`·통합 테스트 실행 | ✅ 2026-11-04 13시 | 개발은 PG16/PostGIS 3.4.2 에서만 통과 Mac PG17.7: migrate·시드(5,093링크, 수집 링크 4개 모두 존재)·적재 3회 통과, 재적재 멱등(스테이징 행 수 = DB 행 수). | runbook step6-gold.md 2~3절 |
| D-73 | `spark-gold` 컨테이너(Java 17) 실행·메모리, Spark 가 Silver 폴더(스트리밍 기록)를 배치로 읽을 때 `_spark_metadata` 존중 여부 | 일부 ✅ 2026-11-04 | 샌드박스(Java 21)에서는 일반 Parquet 로만 테스트. D-21 연장 spark-gold(Java 17) 실행 성공(스테이징 약 1분 30초), Silver 폴더 배치 읽기 오류 없음. **Silver 행 수 대조(silver_verify)는 ⏳**. | 런북 3절 실행 후 `row_counts` 를 silver_verify 와 대조 |
| D-74 | 공휴일 데이터 | ⏳ | `dim.dim_date.is_holiday` 전부 false → 베이스라인이 공휴일을 평일로 취급 | 공공데이터(특일 정보) 승인 후 시드 마이그레이션 |
| D-75 | 버스 정류소 통과(next_stop_id 전이) 팩트 | ⏸ | 의미 미확인(D-57)이라 만들지 않음. `bus_position` 원자료는 적재됨 | 계약 v2 이후 |
| D-76 | 돌발 `absent_since` 의 의미 | ❓ | '성공·비절단 호출에서 처음 안 보인 시각'일 뿐 해제 시각이 아님(D-38) | 하루 이상 쌓인 뒤 expected_clear_at 과 비교 |
| D-77 | audit 에서 `skipped`(예산 소진·회로 차단) 기록은 적재하지 않음 | ⏳ | ingest_id 가 없어 PK 불가 | Step 8 DQ 설계 시 별도 표 |
| D-78 | 베이스라인 임계값(provisional 4주 기준, 혼잡 단계) | ⏸ | 뷰는 있고 임계값 판단은 ADR-0009 | 4주 데이터 이후 |
| D-79 | 로그인 역할(gold_writer 등) 비밀번호 설정 | 👤 | 마이그레이션은 NOLOGIN 역할만 만든다 | runbook 4절 (선택) |
| D-80 | 수집 링크 4개 / 450초 하루 768 호출이 실제로 한도 안인지 | ❓ 진행 | 한도 1,000 은 미확인(D-51) 수집기 재시작 후 `collector status`: 도로소통 계획 768회/일(450초). 서울 포털 호출 통계와의 비교는 아직. | 서울 포털 호출 통계와 `collector status` 비교 |

| D-81 | 수집기는 시작할 때만 sources.yaml 을 읽는다 → 설정 변경 뒤 재시작을 잊으면 옛 설정으로 계속 수집 (11-04: 링크 1개로 13시까지 수집됨) | 운영 규칙 | 재시작 후 `python -m collector status` 의 '계획 N회/일'로 확인. 새 링크 3개는 11-04 13:3x KST 부터 쌓임 |
