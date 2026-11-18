# 미완료·미확인 장부 (DEFERRED)

"아직 안 한 것 / 확인 못 한 것 / 일부러 미룬 것"을 **어디에 추가할지**와 함께 적는 단 하나의 목록이다.
새 채팅은 HANDOFF.md 와 함께 이 파일을 읽는다. 항목을 끝내면 상태를 바꾸고 날짜를 적는다 (지우지 않는다 — 이력이 곧 근거다).
마지막 갱신: 2026-11-18 (저녁: D-254·D-259 닫음, D-266·D-267 추가 · 문서 정합 정리: 결번 D-162·D-164 행 정리, 정의 없는 번호 참조 제거) · 그 앞 2026-11-17 (Step 12 Azure 서빙 계층 배포·파괴·재배포·파괴 완료, P장 추가. 그 앞 갱신: 11-12 O장 · 11-05 Step 10 J장).

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
| D-07 | `spark-silver` 컨테이너 | 가동 ✅, 실측 ✅ 2026-11-04 14시 | Mac 실측: Silver **1.239 GiB / 한도 1.5 GiB (83%)** — 여유 적음, 계속 지켜볼 것. Docker VM 은 사용자가 약 10.7 GiB 로 올림(합계 문제는 해소). Airflow 추가 후 합계는 D-90 | `docker-compose.yml` | `scripts/stack.sh stats` 로 실측해 ADR 실측표에 기록 |
| D-08 | Bronze→Silver 행 수 등식 DQ | ✅ 2026-11-04 (Mac 통과) | 한계: 비교 기준이 Silver 마지막 `fetched_at` −15분이라 데이터가 끊기면 마지막 15분은 비교 밖. "수집 = 정상 토픽 + dlq" 는 아직 이 스크립트에 **없음** | Step 8 DQ | dlq 포함 등식은 Step 8 |
| D-09 | 수집 공백 표시 | ✅ 기록·알림 2026-11-05 (복구는 불가)  — 샌드박스 검증, Mac 확인 대기 (D-130) | 공백 2회, 둘 다 Mac 덮개 닫힘 잠자기와 시각이 일치(`pmset -g log`, 11-04): ① 16:22:58 Clamshell Sleep → 17:59:55 Wake (공백 16:26~18:00) ② 20:12:53 Clamshell Sleep(버스 마지막 수집 20:12:22) → 00:20:59 Wake(중간 DarkWake 때만 일부 수집). Kafka·raw 어디에도 없어 **복구 불가** | Silver 메타 또는 Step 8 DQ 신선도 규칙 (`config/dq_rules.yaml`) | `fetched_at` 간격으로 공백 구간 목록 산출 |
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
| D-32 | `dlq` 에 실제 메시지가 들어왔을 때 Bronze 표 생성·검증 | ❓ 훈련 도구 준비됨 2026-11-05 — Mac 에서 실행 대기 | 일부러 계약 위반 응답을 만들어 흘리기 | Step 8 `tests/recovery/` |
| D-33 | 배치 도중 kill 반복(`docker kill` 여러 번, 타이밍 변경) 후 `bronze_verify` | ❓ 훈련 도구 준비됨 2026-11-05 — Mac 에서 실행 대기 | `docs/runbooks/step4-bronze.md` 5절 반복 | ADR-0005 검증 상태, Step 8 recovery |
| D-34 | 장시간 중단(3~5분 이상) 후 따라잡기 — `maxOffsetsPerTrigger` 가 나눠 읽는지 | ❓ 훈련 도구 준비됨 2026-11-05 — Mac 에서 실행 대기 | 컨테이너를 내렸다 켜고 로그·`bronze_verify` | ADR-0005 |
| D-35 | Bronze 메모리 장시간 추이 | ❓ | 610 MiB(9분) → **846 MiB(11-04 14시, 몇 시간 뒤)**: 계속 늘고 있음. 하루 후 다시 `stack.sh stats` (한도 1.5 GiB) | ADR-0005 실측표 |
| D-36 | 11-02 raw 가 왜 없나 (HANDOFF 는 11-02 12:24 부터 수집했다고 기록) | ❓ | 사용자가 폴더를 옮겼는지 기억/`data/` 아래 다른 위치 확인 (`find data -name '*.gz' -path '*11/02*'`) | 이 장부 |
| D-37 | 수집기 정지 원인 | ✅ 2026-11-04 | 덮개 닫힘 잠자기(D-09). 반복되므로 Step 8 알림 대상이자 상시 가동 방법 필요(D-44) | D-09 |
| D-38 | 돌발 "해제 시각 = 목록에서 사라진 첫 관측"인지, 같은 `acc_id` 내용 해시가 하루 동안 바뀌는 빈도 | ❓ | Silver 돌발 관측 로그가 하루 이상 쌓인 뒤 `exp_clr_*` 와 비교 | `docs/api-catalog/seoul_incident.md`, Gold(Step 6) |
| D-39 | 도로소통 `link_id` ↔ 돌발 `link_id` 조인 가능 여부 | ❓ | **도로소통은 링크 1개만 수집** 중이라 지금은 맞을 수가 없다. 링크 목록 확보 후 검증 | `docs/api-catalog/`, Step 6 `dim_link` |
| D-40 | 링크 지오메트리 + 수집 링크 목록 | ✅ 2026-11-04 (6-0) | LINK_VERTEX_2025(5,093링크, EPSG:5181)를 `dim.dim_link` 시드로 사용(`batch/seed_dim_link.py`, 파일은 git 제외). 수집 링크 4개로 확대(sources.yaml, 450초) / 이전: 후보 "소통 돌발 도로별 링크 정보" 데이터셋. 방법은 `docs/runbooks/step0-closeout.md` 4장 | `config/sources.yaml`(링크 목록), `sql/`(`dim_link`) — Step 6 전 필수 |
| D-41 | `RN1` 비 오는 날 응답 모양 | ❓ (H9) | 현재 `RN1` 원문은 `"0"` 6건뿐. Silver 는 `value_raw`+`value_num`+`value_unparsed` 로 대비. 비 오는 날 D-00 재실행 → `RN1_raw_values` | `docs/api-catalog/kma_ncst.md` |
| D-42 | 심야 버스 응답 모양(0대 응답 등) | ❓ (H9) | 버스는 04~24시만 수집. 23~24시대 D-00 재실행 | `docs/api-catalog/bus_position.md` |
| D-43 | 버스도착 `arrive_code=2` (99건/17,526)의 뜻 | ❓ | 원문 보존. Silver 생성 후 `arrive_code=2` 행의 `arrival_msg` 등 비교 | `docs/api-catalog/bus_arrival.md`, 계약 v2(D-57) |
| D-44 | 컨테이너 시계가 호스트보다 느렸던 구간(`ingested_at - fetched_at` 최솟값 -1,906초) | ❓ 가설 | 11-04 00:39 현재 호스트(15:36:27Z)와 컨테이너(15:39:32Z, 3분 뒤 측정)는 **일치**. 과거 구간의 어긋남 원인은 여전히 미확인(가설: 잠자기 뒤 Docker VM 시계 지연). Silver 는 `ingested_at<fetched_at` 행에 `ingest_clock_skew` 플래그만 단다. 수집기는 컨테이너가 아니라 호스트 프로세스(`docker ps` 에 없음). 상시 가동: AC 전원+덮개 열기, 또는 `sudo pmset -a disablesleep 1`(비공식·미검증, 끝나면 0) | `docs/runbooks/`, ADR-0005 |
| D-45 | Silver 시간 역전·미래 event_time 0 확인 | ✅ 2026-11-04 Mac 실행: **미래 시각 0·시간 역전 0** | `silver_verify` 에 표마다 `미래 시각`·`시간 역전`(버스위치 차량별) 추가, 테스트·변이 시험 통과 | `streaming/silver_verify.py` | Mac 에서 1회 실행해 둘 다 0 |
| D-46 | 돌발 변환 좌표 육안 확인 | 코드 ✅, Mac 출력 6줄 받음 — 지명 대조는 사용자 판단 대기 | `silver_verify` 출력에 `[좌표 확인용]` 6줄(기간 긴 순) | `docs/api-catalog/seoul_incident.md` | 지명과 좌표가 맞는 건 5건 이상(표본이 6건뿐이면 가능한 만큼) |

## D. Step 3·0 에서 넘어온 선택 항목 (HANDOFF 9장)

| ID | 항목 | 상태 | 어디에 추가할지 |
|---|---|---|---|
| D-50 | 기상청 `resultCode 03`(NO_DATA)을 오류가 아닌 "발표 전" 상태로 분류 | ✅ 2026-11-04 (6-0) | KMA 03 및 버스위치 headerCd=4 를 `empty` 로 분류(실패 아님). 픽스처는 재구성본 / 이전: `src/urbanstream/contracts/weather.py`, `collector/runner.py`, 테스트 — Step 8 전 |
| D-51 | 서울 도로소통·돌발 호출 한도 확인(미확인, 보수적 1,000건) | 👤 | `config/sources.yaml`, ADR-0004 결정 7 |
| D-52 | 포털 호출 통계 vs `python -m collector status` 비교(상세기능 이름 `getBusPosByRtidList` vs `getBusPosByRtid`) | 👤 | ADR-0004 |
| D-53 | 도로소통 간격 120초를 퇴근 시간대 측정으로 재확인 | 👤 (선택) | `docs/api-catalog/seoul_traffic.md` |
| D-54 | 노선정보(15000193)·정류소정보(15000303) 승인 확인 후 `seoul-bus-ids` 재실행, 노선 범위 결정(노선 N개면 간격 N배) | 👤 | `config/sources.yaml` |
| D-55 | 새 코드 장시간 검증: audit 의 `attempts>=2`·`TotalTimeout` 확인 | ✅ 도구 2026-11-05 (ops.call_event · DQ call_fail_rate) — Mac 데이터로 값 확인 대기 | Step 8 `health_stream` (그 전엔 `platform.audit` 토픽 grep, HANDOFF 3장 명령) |
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
| D-71 | `.gitignore` 결함: `data/*` + `!data/*/` 때문에 data/ 안쪽 파일이 추적 대상이었다 (임시 저장소로 재현) | 👤 일부 | 규칙은 `data/**` 로 수정·테스트함. data/ 추적 해제 커밋 완료. 과거 이력에 data/ 파일 약 6,500개(checkpoints 3,354 · bronze 2,111 · raw 580 · silver 333 · smoke 162 …), **`.env` 는 이력에 없음(확인)**. 사용자가 `../urbanstream-clean` 에서 `git filter-repo` 완료(data/ 0개, 파일 180, 커밋 3, origin 제거됨). **남은 일: 새 비공개 레포로 push 할지 / 기존 레포 force push 할지 결정**, `data/**/.gitkeep` 뼈대 복원, 작업 레포를 새 이력에 맞추기(`git remote set-url` → `fetch` → `reset --mixed origin/main`). 기존 레포 force push 시 GitHub 가 옛 객체를 얼마나 보관하는지는 **미확인** → 새 레포 권장 | Mac: `git ls-files data` 가 비어야 함. 나오면 `git rm -r --cached data` 후 커밋, **이미 push 했다면 레포가 비공개여도 이력에 남음** → 알려 주세요 |
| D-72 | Mac(PG17.7/PostGIS 3.6.1)에서 `migrate.py`·통합 테스트 실행 | ✅ 2026-11-04 13시 | 개발은 PG16/PostGIS 3.4.2 에서만 통과 Mac PG17.7: migrate·시드(5,093링크, 수집 링크 4개 모두 존재)·적재 3회 통과, 재적재 멱등(스테이징 행 수 = DB 행 수). | runbook step6-gold.md 2~3절 |
| D-73 | `spark-gold` 컨테이너(Java 17) 실행·메모리, Spark 가 Silver 폴더(스트리밍 기록)를 배치로 읽을 때 `_spark_metadata` 존중 여부 | ✅ 2026-11-04 14시 | spark-gold(Java 17) 실행 성공: 스테이징 **약 56초**, 메모리 표본은 **768 MiB 에서 잘림 → 최대치 미확인**(한도 1.5 GiB 안에서 성공). Silver 폴더 배치 읽기 오류 없음, silver_verify 통과. Airflow 에서는 스케줄러 컨테이너 안에서 Java 17/21 로 돈다 → D-90 | 런북 3절 실행 후 `row_counts` 를 silver_verify 와 대조 |
| D-74 | 공휴일 데이터 | ⏳(2026 일부 시드 D-180) | `dim.dim_date.is_holiday` 전부 false → 베이스라인이 공휴일을 평일로 취급 | 공공데이터(특일 정보) 승인 후 시드 마이그레이션 |
| D-75 | 버스 정류소 통과(next_stop_id 전이) 팩트 | ⏸ | 의미 미확인(D-57)이라 만들지 않음. `bus_position` 원자료는 적재됨 | 계약 v2 이후 |
| D-76 | 돌발 `absent_since` 의 의미 | ❓ | '성공·비절단 호출에서 처음 안 보인 시각'일 뿐 해제 시각이 아님(D-38) | 하루 이상 쌓인 뒤 expected_clear_at 과 비교 |
| D-77 | audit 에서 `skipped`(예산 소진·회로 차단) 기록은 적재하지 않음 | ✅ 2026-11-05 (ops.call_event 가 모든 audit 이벤트를 보존) | ingest_id 가 없어 PK 불가 | Step 8 DQ 설계 시 별도 표 |
| D-78 | 베이스라인 임계값(provisional 4주 기준, 혼잡 단계) | ⏸ | 뷰는 있고 임계값 판단은 ADR-0009 | 4주 데이터 이후 |
| D-79 | 로그인 역할(gold_writer 등) 비밀번호 설정 | 일부 ✅ Step 7 | Airflow 는 `gold_loader`(LOGIN, gold_writer 상속)로만 접속, 비밀번호는 `init-env` 가 생성·`db_bootstrap.py` 가 설정. 호스트에서 수동 적재(`gold_run.sh`)에 쓸 `gold_writer` 직접 로그인은 여전히 선택 | runbook 4절 |
| D-80 | 수집 링크 4개 / 450초 하루 768 호출이 실제로 한도 안인지 | ❓ 진행 | 한도 1,000 은 미확인(D-51) 수집기 재시작 후 `collector status`: 도로소통 계획 768회/일(450초). 서울 포털 호출 통계와의 비교는 아직. | 서울 포털 호출 통계와 `collector status` 비교 |

| D-81 | 수집기는 시작할 때만 sources.yaml 을 읽는다 → 설정 변경 뒤 재시작을 잊으면 옛 설정으로 계속 수집 (11-04: 링크 1개로 13시까지 수집됨) | 운영 규칙 | 재시작 후 `python -m collector status` 의 '계획 N회/일'로 확인. 새 링크 3개는 11-04 13:3x KST 부터 쌓임 |

## G. Step 7 (Airflow + Spark 배치) — 11-04

| ID | 항목 | 상태 | 현재 알려진 것 | 확인 방법 / 어디에 |
|---|---|---|---|---|
| D-90 | Airflow 3 컨테이너 실제 메모리, Docker VM 합계 | ✅ 2026-11-04 19시 Mac 실측 | 스케줄러(Spark 포함) 피크 **1.50 GiB / 3 GiB**(5초 간격 표본), 끝난 뒤 ~0.68 GiB. API 서버 0.30, DAG 프로세서 0.22 GiB. 실행 중 Silver 1.04~1.10 GiB(한도의 73% 이하). 합계 약 4.9 GiB vs Docker VM 10.9 GiB → 여유 큼 | 한도 조정은 불필요. 데이터가 늘면 `gold_stage` 시간·메모리 재측정 |
| D-91 | Airflow 이미지 빌드 가정: 베이스가 Debian 계열(`apt-get`), FabAuthManager 포함(`airflow users create`), arm64 | ❓ | 공식 compose 가 같은 인증 관리자를 쓰므로 포함돼 있을 가능성이 높지만 **이미지에서 직접 확인 못 함** | 첫 `up-airflow` 빌드 로그. 실패 시 Dockerfile 의 apt 줄 또는 `AUTH_MANAGER` 변경 |
| D-92 | Compose v2.10.2(구버전)에서 scheduler→(apiserver, dag-processor) `service_healthy` 연쇄 | ❓ | `service_completed_successfully` 는 쓰지 않음. 스케줄러 start_period 120초(마이그레이션 시간 포함) | `up-airflow` 가 `--wait` 로 끝나는지. 안 끝나면 `docker compose logs airflow-scheduler` |
| D-93 | 예약 실행(schedule)·KST 동작 | 일부 ✅ 11-04 Mac: 예약 실행이 저절로 돌고 `gold_stage 시작` 날짜가 오늘(KST). **자정 이후(00시 KST) 날짜 전환은 미확인** | 샌드박스는 `airflow dags test` 수동 실행만 검증. 수동 실행은 `data_interval_end = logical_date` 가 채워짐(사실). 예약 실행의 `data_interval_end` 가 KST 로 올바른 날짜가 되는지는 **Mac 에서 시계로 확인** | DAG 를 unpause 하고 20분에 실제로 도는지, 로그의 `gold_stage 시작: <날짜> ~ <날짜>` 가 오늘(KST) 인지 |
| D-94 | DAG 는 만들어질 때 일시 중지 상태 | 👤 | `DAGS_ARE_PAUSED_AT_CREATION=true`(실수로 켜지는 것 방지) | UI 에서 3개 토글 켜기 또는 `airflow dags unpause` (런북) |
| D-95 | 에피소드는 매 실행 때 **전체 이력**으로 다시 계산 | ⏸ 위험 | 데이터가 쌓일수록 `gold_stage` 시간이 늘고, 나중에 보관 삭제(D-23/D-24)를 하면 전체 이력 재계산과 충돌(삭제된 과거로 에피소드가 달라짐) | 소요 시간을 `gold_intraday` 로그로 추적. 보관 정리 설계 때 "에피소드 확정(7c `incident_impact_finalize`) 후 삭제" 순서로 |
| D-96 | 팩트는 DO NOTHING → "범위 교체(replace-range)" 재계산 없음 | ⏸ | Silver 규칙이 바뀌어 다시 쌓아도 이미 적재된 팩트 행은 안 바뀐다. 날씨·에피소드·감사만 갱신 | 필요해지면 `backfill` 에 `replace` 모드(해당 날짜 삭제 후 삽입, 트랜잭션) 추가 |
| D-97 | `ops.load_run` 이 SIGKILL 뒤 `running` 으로 남을 수 있음 | ✅ 2026-11-05 (DQ load_health 가 오래된 running 을 fail 로 알림) — Mac 확인 대기 | `gold_daily` 의 housekeeping 이 120분 넘은 `running` 을 `failed` 로 표시(`mark_stale_runs`). 단위·통합 테스트 통과 | Step 8 DQ 에서 표시 상태를 경보로 |
| D-98 | `row_counts` 는 삽입 행이 아니라 스테이징 행 | ✅ 보완 | `verify_counts` 가 DB 의 날짜 범위 행 수 ≥ 스테이징 행 수를 확인 | — |
| D-99 | `backfill_from_raw` (Bronze→Silver 재생성) | ⏸ 설계만 | ADR-0009 §3: 출력은 별도 루트 `silver_backfill`, `gold_stage` 가 두 루트를 합침 | 필요해질 때 |
| D-100 | `dq_report_daily`, `silver_reconcile_hourly`, `platform_supervisor` | ✅ 2026-11-05 (dq_report_daily · silver_reconcile_daily(시간→일 단위로 바꿈) · platform_supervisor→monitor 컨테이너) | 알림 함수 시그니처(`urbanstream.notify.send`)는 고정됨. 지금은 로그 + 비밀 마스킹 | Step 8 |
| D-101 | `reference_data_sync` (공휴일, D-74) | ⏸ | 공공데이터 승인 필요 | 승인 후 |
| D-102 | `api_reader` 에 `ops` 접근 없음 | ✅ ops 뷰 GRANT 2026-11-05 (엔드포인트는 Step 9) | API 가 DAG/적재 상태를 보여주려면 ops 뷰와 GRANT 가 필요 | Step 9. 새 표·뷰는 같은 마이그레이션에서 `api_reader`·`bi_reader` 에 GRANT (DEFAULT PRIVILEGES 안 씀) |
| D-103 | 7b: 구간 통과 시간의 `sect_ord` 가정, 실제 데이터 값 | ✅ Mac 실데이터로 지지 (11-04) | Mac 결과: 4,815행·102개 구간·통과 시간 p50 100초·p90 257초·최소 60초·최대 982초 → 구간 경계에서 `sect_ord` 가 1씩 증가한다는 가정과 모순 없음(그럴듯한 값). 노선별 구간 수와의 대조는 하지 않았음 → 정류소 간 시간 방식으로 바꿀 필요 없음, 이상 징후가 보이면 재검토 | 필요 시 노선별 구간 수와 `count(distinct section_id)` 비교 |
| D-104 | Mac 잠자기(덮개 닫힘)로 Airflow 실행이 중단·실패 | ✅ 분류 구현 2026-11-05 (host_resume + 침묵 판정 + 격하/묶기) · monotonic 전제 [추정] 은 Mac 확인 대기 (D-120) | 11-04 Mac 에서 실패 2건(수동 15:24 KST, 예약 07:20 UTC)이 `pmset` 잠자기 구간(15:25~17:00, 18:04~18:35 KST)과 시각이 맞음. 로그에 오류 줄이 없어 **원인은 추정**. 다음 실행이 같은 윈도우를 다시 처리해 데이터 손실은 없음(load_run 5~7 ok) | 전원 연결·`caffeinate -i`·덮개 열기. Step 8 알림에서 잠자기 구간 실패는 경고로 낮출지 결정 |
| D-105 | 7b: 드리프트는 시간표 대비 지연이 아니다 | ✅ 설계 | 마지막 예측이 실제 도착에 가깝다는 가정의 대리 지표. 실제 도착 시각 기반 예측 오차는 D-57(`next_stop_id` 전이 의미)이 풀린 뒤 | 계약 v2 이후 |
| D-106 | 7b: 임계값이 `speed_zero` 행을 포함하고 공휴일을 평일로 취급 | ⏸ | 기준선 MV 와 같은 규칙. 0 의 의미(정체 vs 결측)가 미확인(D-?) / 공휴일 D-74 | 의미 확인 후 `dq_flags` 로 분리, 공휴일 데이터 후 `day_type='holiday'` 자동 생성 |
| D-107 | 7b: `ops.load_run.job` 이 항상 'gold_load' | ✅ 2026-11-05 (ops.load_run.dag_id 추가, job 은 'gold_load' 유지 — D-128) | 선행 조건 검사가 DAG(gold_daily/intraday/backfill)를 구분하지 못하고 날짜 범위로만 판단 | Step 8 에서 `job` 에 DAG 이름 기록 검토 |
| D-108 | 7b: 새 마트 표·뷰의 API 노출 | ⏸ Step 9 | `api_reader`·`bi_reader` 에 SELECT 는 이미 GRANT. 엔드포인트와 대시보드는 Step 9~10 | — |
| D-109 | 7c: Mac 실데이터 값 | ⏳ Mac | 돌발 영향·날씨×교통·표 통계가 Mac 에서 값이 나오는지 미확인. 샌드박스는 합성 데이터 통합 테스트 6개 + 변이 4건 + `airflow dags test` 3개(빈 데이터) | 런북 7c 4번 결과를 알려 주기 |
| D-110 | 7c: `VACUUM` 은 하지 않음(autovacuum 에 맡김) | ⏸ | `gold_writer` 는 표 소유자가 아니고 `VACUUM` 은 함수 안에서 못 한다. `ANALYZE` 만 소유자 권한 함수 `ops.analyze_warehouse()` 로 한다. 밀리는지는 `ops.table_stats.n_dead_est` 로 본다 | `n_dead_est` 가 `n_live_est` 의 20% 를 넘는 표가 계속되면 소유자 계정으로 수동 VACUUM 또는 PG17 `MAINTAIN` 권한 |
| D-111 | 7c: 기준선이 비교 대상 기간을 포함 | ⏸ | `mv_link_baseline` 은 전체 이력으로 만들어져 영향/날씨 비교 구간의 속도도 기준선에 들어간다. 데이터가 적을수록(지금은 몇 주 미만) 비율이 1.0 쪽으로 눌린다. 4주 미만이면 `provisional` | 4주 이상 쌓이면 문제 B·D 에 "해당 구간을 뺀 기준선" 옵션을 검토 |
| D-112 | 7c: 돌발 → 링크 연결률이 낮을 수 있음 | ⏳ Mac | 수집 링크 4개뿐이라 대부분 `no_link`. 반경 300 m 와 24시간 확정 대기는 합의한 값이며 데이터로 검증 전. `absent_since` 는 해제 시각이 아니라 첫 미관측(ADR-0008 ⑧). 기상 PTY 눈 코드(2,3,6,7)는 코드표 기준 [추정] | 런북 7c 4번의 연결 건수 / 링크 확대 때 재검토 |
| D-113 | 7c: Bronze/Silver 파일 정리는 넣지 않음 | ⏸ | Airflow 는 해당 폴더를 읽기 전용으로만 본다(D-21). D-24 와 같은 항목 | 디스크 점검 시 별도 결정. 돌발 에피소드 원본 삭제는 `mart.incident_impact` 확정 이후에만(D-95) |
| D-114 | 7c: 샌드박스 테스트가 관리자 계정이라 `gold_writer` 권한 누락을 놓침 | ✅ 수정 (11-04 Mac 발견) | `mart.mv_link_baseline` 읽기 권한이 없어 문제 B·D DAG 가 `permission denied` 로 재시도 루프. `sql/008_grant_baseline.sql` 추가 + 모든 빌더를 `gold_writer` 로 돌리는 테스트(`test_every_builder_runs_as_gold_writer`). 같은 종류의 누락은 앞으로 이 테스트가 잡는다 | Mac 에서 008 적용 후 재실행 (런북 7c) |
| D-115 | 7c: 기준선 셀이 없는 시간대에서 `sufficient` 가 NULL → 날씨 DAG 실패 | ✅ 수정 (11-04 Mac 발견) | 기준선 MV 는 매시 갱신이라 가장 최근 시간대는 셀이 없다. `AND` 식이 NULL 이 되어 NOT NULL 위반. `coalesce(…, false)` + 회귀 테스트(`test_hour_without_baseline_cell_is_insufficient_not_null`). 같은 형태의 불리언 NOT NULL 열은 다른 빌더에서 점검함(문제 없음) | Mac 에서 다음 재시도 때 성공하는지 |


## H. Step 8 (DQ · 모니터링 · 알림 · 이상 탐지) — 11-05

모든 항목은 **샌드박스(Postgres 16 · Airflow 3.3.0 DAG 파싱 · 가짜 Kafka·가짜 웹훅 서버)에서만 검증**했다. 실제 Kafka, Docker 이미지 빌드·기동, Mac 잠자기, 실제 Discord/Slack 전송은 Mac 에서 확인 전이다.

| ID | 항목 | 상태 | 현재 알려진 것 | 확인 방법 / 어디에 |
|---|---|---|---|---|
| D-120 | 잠자기 감지 전제: macOS 의 `time.monotonic()` 이 잠자는 동안 멈춘다 | ❓ [추정] | 수집기는 벽시계 경과와 단조시계 경과의 차이로 `host_resume` 을 낸다. 단조시계가 멈추지 않으면 차이가 0 이라 **감지 못 한다**(오탐은 없음 — 그 경우 침묵 판정 `collector_blackout` 600초 규칙과 DAG 실패 원래 수준 유지로 안전하게 떨어진다) | 👤 `python scripts/sleep_probe.py` 를 켜 둔 채 덮개를 1~2분 닫았다 열기 → '잠잔 시간' 이 닫은 시간과 비슷하면 전제가 맞다 |
| D-121 | 모든 임계 숫자가 [가정] 시작값 | ⏳ 7일 후 | `config/alerts.yaml`(쿨다운·시간당 상한·유예), `config/monitor.yaml`(2.5×/5× 신선도·실패율), `config/dq_rules.yaml`(범위·비율·커버리지), `batch/anomaly.py`(z 3.5, 하한 2 km/h). DQ 는 `shadow_until: 2026-11-12` 까지 기록만 한다 | 7일 뒤 `SELECT rule_id, status, value FROM ops.v_dq_latest`, `ops.source_health` 분포를 보고 값을 조정한 뒤 `shadow_until` 을 앞당기거나 규칙에 `mode: enforce` |
| D-122 | Kafka 컨슈머 지연(lag)을 정확히 재지 않는다 | ⏸ | 소스 신선도는 audit 시각, 레이어 정체는 parquet 파일 mtime 으로 본다. 브로커의 end offset 과 Spark 체크포인트를 비교하지 않는다 | Step 10 에서 Prometheus 를 쓰게 되면 지표로 추가 |
| D-123 | 웹훅은 best-effort | ⏸ | 1회 재시도 후 포기하고 `ops.alert_event.status='failed'` 로 남기며, 다음 푸시 성공 때 "이전 전달 실패 N건"을 함께 알린다. 이메일·SMS 등 두 번째 채널 없음. 웹훅 서비스가 죽은 동안의 알림은 아침 요약에도 안 간다(failed 는 digest 대상 아님) | 필요하면 `notify/transport.py` 에 채널 추가 |
| D-124 | 이상 탐지 지연: 발생 후 최대 약 1시간 20분 | ✅ 문서화 | Gold 가 매시 20분에 적재 → `anomaly_scan` 이 35분에 본다. 실시간이 아니다 | 실시간이 필요하면 Silver 를 직접 읽는 스트리밍 탐지(별도 설계) |
| D-125 | 기준선이 28일 미만인 동안 이상은 알림이 아니다 | ✅ 설계 | 수집 시작 2026-11-02 → 기준선이 성숙(provisional=false)해지는 가장 이른 날은 약 11-30. 그 전의 이탈은 `candidate` 로 기록만 | `SELECT status, count(*) FROM mart.anomaly_event GROUP BY 1` |
| D-126 | 감시자(monitor) 자체가 죽으면 알려 줄 곳이 없다 | ⏸ | 컨테이너 healthcheck(박동 파일 5분)와 `restart: on-failure:3` 뿐. 외부 데드맨 스위치 없음. 컨테이너 메모리(`MONITOR_MEM_LIMIT` 256m)는 **실측 전** | 👤 `scripts/stack.sh stats` 로 monitor 메모리 확인 후 한도 조정. 외부 감시가 필요하면 Step 10 |
| D-127 | 수집기 쪽 공백은 복구할 수 없다 | ✅ 기록 · ⏸ 복구 | `ops.data_gap`(recoverable=false)에 기록하고 DQ 커버리지가 공백으로 설명되면 fail 대신 warn. Bronze→Silver 재생성(`backfill_from_raw`, D-99)은 설계만 있고 구현 안 함. Kafka·raw 에 없는 시간은 API 가 실시간 값만 주므로 되살릴 수 없다 | 하류(Bronze/Silver/Gold) 공백은 다음 실행이 스스로 메운다 |
| D-128 | `ops.load_run.dag_id` 추가, `job` 은 'gold_load' 유지 | ✅ | HANDOFF 7-8 의 "job 에 DAG 이름" 안은 마트 선행 조건(`job='gold_load'` 필터)을 깨서 `dag_id` 열을 덧붙이는 쪽으로 바꿨다. 백필은 `backfill_<날짜>` 로 기록된다(스테이징 폴더 이름과 같은 값) | `SELECT dag_id, count(*) FROM ops.load_run GROUP BY 1` |
| D-129 | Prometheus·Grafana 는 만들지 않았다 | ⏸ Step 10 | Docker VM 메모리(약 4.9/10.9 GiB)를 아끼려고 상태를 Postgres 표(`ops.source_status` 등)에 둔다. Grafana 는 나중에 이 표를 바로 읽을 수 있다 | Step 10 |
| D-130 | Mac 에서 확인할 것(8a) | 👤 | ① `scripts/stack.sh up-monitor` 로 이미지 재빌드(confluent-kafka 추가)·기동 ② 호스트→컨테이너 bind-mount 에서 Bronze/Silver 파일 mtime 이 보이는지 ③ 실제 Kafka `platform.audit` 읽기 ④ 실제 웹훅(`scripts/recovery_drill.sh webhook-test`) ⑤ 수집기 재시작(D-81, `host_resume` 반영) ⑥ 잠자기 시 `host_stall` 격하 | `docs/runbooks/step8-recovery.md` |
| D-131 | DQ 게이트 시차 | ⏸ | DQ 는 05:00 에 도는데 마트는 02:00~03:30 에 돈다 → 마트의 `require_dq_ok` 는 **전날 DQ 결과**를 본다. 관찰 기간(~11-12) 동안은 어떤 규칙도 막지 않는다 | 필요하면 DQ 를 02:00 앞으로 옮기기(gold_daily 01:00 뒤) |
| D-132 | `silver_reconcile_daily` 는 Airflow 컨테이너 안에서 `streaming.silver_verify` 를 서브프로세스로 돌린다 | ❓ Mac | 출력 줄 형식(`silver_xxx OK/FAIL ...`)을 파싱한다. 형식이 바뀌면 `unknown(no_output)` 으로 남고 알림이 가지 않는다(조용히 실패하지 않도록 unknown 은 요약에 표시됨). 컨테이너에 `/opt/urbanstream/streaming` 마운트는 이미 있음 | 👤 `docker exec urbanstream-airflow-scheduler airflow dags test silver_reconcile_daily` |
| D-133 | DQ 규칙의 의미 가정 | ❓ [추정] | `bus_position.next_stop_id ∈ bus_arrival.station_id`(정류장 ID 체계가 같다는 가정, 기록만), 시간대 커버리지 창(버스 5~23시), 위경도 범위(한국 bbox) | 7일 관찰 결과로 확인 |
| D-134 | DAG 실패 알림의 작업 시작·종료 시각 | ❓ Mac | `ti.start_date`/`end_date` 가 콜백 컨텍스트에 있다고 가정(Airflow 3). 없으면 알림 생성 10분 전~생성 시각을 창으로 쓴다 | 👤 잠자기 훈련 때 `SELECT meta FROM ops.alert_event` 로 started_at/ended_at 확인 |
| D-135 | 컨테이너 시계가 잠자기 뒤 늦을 수 있다(D-44) | ✅ 대응 | 수집 시각은 호스트가 찍은 값(`recorded_at`)을 기준으로 하고, 감시 쿼리는 `now` 상한을 두지 않는다. 컨테이너 시계가 늦어도 호스트 시각의 이벤트를 놓치지 않는다 | — |
| D-136 | 처음 켠 날의 아침 요약 건너뜀 | ✅ 설계 | 요약 시각(07:30) 이후에 처음 켜면 그날 요약은 보내지 않는다(재설치 때 알림 폭탄 방지) | — |
| D-137 | 실제 Discord/Slack 응답 형식 | ❓ | 시험은 로컬 가짜 서버로 JSON 본문(`content`/`text`)과 마스킹만 확인했다. 실제 서비스의 속도 제한(429)·본문 길이 제한(Discord 2000자)은 문서값으로 처리했다(1900자 절단) | 👤 `scripts/recovery_drill.sh webhook-test` |
| D-138 | Mac 첫 실행에서 발견한 권한 누락 | ✅ 수정 (Mac 재확인 대기) | `gold_writer` 가 `mart.v_congestion_threshold_latest` 를 못 읽어 DQ 규칙 `congestion_threshold_reliability` 가 check_error(→unknown)로 묻혔다. `sql/012_grant_mart_views.sql` 로 mart 뷰 전체 SELECT 부여 + 실제 규칙 전부를 gold_writer 로 돌려 check_error 가 없는지 보는 테스트 + 모든 mart 뷰 읽기 테스트 추가. 교훈: 규칙 오류가 unknown 으로 흡수되므로 'unknown 개수'가 아니라 'check_error 없음'을 검사해야 한다 | 👤 `python scripts/migrate.py` 후 DQ DAG 재실행, `ops.v_dq_latest` 에서 해당 규칙이 unknown 이 아닌지 |
| D-139 | 알림 본문의 시각이 UTC | 🔧 개선 | Mac 훈련(11-05)에서 bad-key 알림의 '최근 성공' 이 `18:39:09+00:00`(UTC)로 표시됐다. 동작에는 문제 없으나 KST 로 읽기 어렵다 | 코드: `notify` 본문을 만드는 `monitor/loop.py` 의 시각 포맷을 KST 로 변환 |
| D-140 | 소스 기대 호출 간격을 interval ÷ 대상 수로 계산한 오류 | ✅ 수정 (Mac 재확인 대기) | 수집기는 대상(링크 4개)을 3초 어긋나게 시작해 같은 간격(450초)으로 거의 동시에 부른다. 모니터가 112초로 가정해 `seoul_traffic degraded` 가 450초마다 반복(Mac 11-05 03:27~03:47). 기대 간격 = interval 로 수정(warn 1125초·down 2250초). 트레이드오프: 도로소통 완전 중단을 알아채는 데 최대 약 37분 | 👤 monitor 재시작 후 `status` 에서 seoul_traffic 이 ok 로 유지되는지 |
| D-141 | 수집 시간대가 막 열릴 때 계층 '적재 정체' 오경보 | ✅ 수정 (Mac 재확인 대기) | Mac 11-05 04:00~04:02: 버스 창이 열리며 소스가 ok 가 되는 순간, 계층의 마지막 파일은 어제 것(약 4시간 전)이라 bronze/silver 4곳이 즉시 [ERROR]. 정체 판정을 max(마지막 파일, 창 시작)부터 재도록 수정(종일 수집 소스는 그대로). 트레이드오프: 창이 열린 뒤 임계값(버스 약 17분)까지는 진짜 정체도 알리지 않는다 | 👤 내일 04:00 직후 알림이 안 오는지(버스 창 열림), 그리고 Spark 훈련(지연 시 정체 알림) |
| D-142 | Kafka 재시작 뒤 spark-bronze 가 영구 정지 | ✅ 수정 (Mac 재확인 대기) | Mac 훈련(11-05 03:45 KST): Kafka 를 내렸다 올리자 Bronze 쿼리가 `No resolvable bootstrap urls`(브로커 이름 해석 실패)로 종료 → `restart: on-failure:3` 이 Kafka 복귀 전에 모두 소진돼 약 8시간 적재 중단(Bronze·Silver 새 파일 없음, 모니터가 [ERROR] 로 알림 — 감시는 정상 동작). 재시작 횟수 제한을 없앴다(`on-failure`). 데이터는 Kafka(보관 7일)에 남아 있어 Bronze 가 체크포인트 오프셋부터 따라잡는다. 트레이드오프: 코드 오류로 계속 죽는 서비스도 계속 재시작한다(Docker 가 간격을 늘린다 — 상한은 [미확인]) → 반복 재시작은 모니터의 정체 알림으로 알아챈다 | 👤 `up-silver` 후 정체 알림 해소, 이후 kafka-down/up 을 다시 해 Bronze 가 스스로 돌아오는지 |
| D-143 | Discord 전달 실패 기록 6건 | ❓ 원인 미확인 | 11-05 11:43 알림에 `(이전 전달 실패 6건)` 이 붙었다. 어느 시점의 어떤 오류(속도 제한 429? 네트워크? 길이?)인지 아직 모른다 | 👤 `ops.alert_event` 의 실패 기록 확인 → Claude 가 분석 |
| D-144 | 알림 안내문의 서비스 이름 오류 | ✅ 수정 | `docker compose logs bronze-job` 처럼 없는 서비스 이름을 안내했다. 실제는 `spark-bronze`/`spark-silver` → `docker compose logs --tail 60 spark-<계층>` | — |
| D-145 | 시간당 푸시 상한이 critical 까지 막음 | ✅ 수정 (Mac 재확인 대기) | Mac 11-05 12:28 Bronze·Silver 복구로 '해소' 알림이 몰려 시간당 10건 상한이 찼고, 12:32 Kafka 다운 critical 이 매분 `suppressed`(억제)돼 7분 넘게 알림이 오지 않았다(모니터는 3회 실패를 정상 감지). critical 은 상한에서 제외. 남은 개선: '해소'(info) 알림이 상한을 채우지 않게 집계에서 빼거나 묶는 것 | 👤 kafka-down 훈련 재시험 |
| D-146 | 사용자 확인 대기 3건 (Step 9 와 무관) | 👤 | ① Spark 중단 훈련(spark-down 후 약 20분 뒤 '적재 정체' 알림 → spark-up → 해소) 결과 보고 ② 내일(11-06) 04:00 버스 창이 열린 직후 오경보가 없는지(D-141) ③ 2026-11-12 이후 DQ 임계값 검토·`shadow_until` 해제(D-121) | `docs/runbooks/step8-recovery.md`, `SELECT rule_id, status, value FROM ops.v_dq_latest` |

## I. Step 9 (FastAPI 서빙) — 11-05

모든 항목은 **샌드박스(Python 3.11 · Postgres 16 + PostGIS · 실제 uvicorn)에서만 검증**했다. Docker 이미지 빌드·컨테이너 기동·Mac 실측은 확인 전이다. 결정: `docs/adr/0011-step9-api.md`, 실행: `docs/runbooks/step9-api.md`.

| ID | 항목 | 상태 | 현재 알려진 것 | 확인 방법 / 어디에 |
|---|---|---|---|---|
| D-147 | Mac 에서 확인할 것(9) | 👤 | ① `scripts/stack.sh up-api` 로 이미지 빌드(python:3.12-slim-bookworm, 의존성 휠)·healthy ② Compose v2.10.2 에서 `api` 서비스 동작 ③ `docker stats` 메모리 — 샌드박스 RSS 62→82 MiB, 한도 `API_MEM_LIMIT` 기본 256m 은 **[추정]** ④ (샌드박스에서 3.11·3.12·3.13 통과 — Mac 의 Python 에서만 재확인) ⑤ 실제 데이터로 `/v1/traffic/current` 응답 시간 | `docs/runbooks/step9-api.md` §2~4 |
| D-148 | 인증은 정적 API 키 | ⏸ | `X-API-Key` 여러 개(교체용). 사용자별 권한·키 만료·감사 로그의 사용자 구분(해시 앞 8자 `key_id` 뿐)은 없다. HTTPS 는 없다(127.0.0.1 바인딩) | 외부에 공개하려면 리버스 프록시(TLS)·키별 권한을 별도 설계 |
| D-149 | "현재" 값은 실시간이 아니다 | ✅ 문서화 | Gold 적재가 매시 20분 → `current` 류는 최대 약 80분 늦다. 모든 응답의 `meta.as_of/age_sec/stale/loaded_at` 로 드러낸다(기본 3시간 넘으면 `stale`) | Step 10 대시보드가 이 값을 화면에 표시해야 한다 |
| D-150 | 속도 제한·캐시가 프로세스 안 | ⏸ | 워커 1개 전제. 재시작하면 초기화, 컨테이너를 여러 개 띄우면 한도가 키별이 아니라 컨테이너별로 적용된다 | 확장이 필요하면 Redis 등 공유 저장소 |
| D-151 | 버스 엔드포인트의 의미 | ✅ 문서화 · ❓ | `arrival-drift`(도착 예측 변동)·`section-slowness`(구간 통과 시간)는 **시간표 대비 지연이 아니다**(처음 제안의 `/delays` 에서 이름 변경). `sect_ord` 가 구간 경계에서 바뀐다는 가정은 D-103(Mac 데이터로 지지됨). 수집 창(04:00~) 밖 `positions` 는 비어 있다 | OpenAPI 설명과 `meta.notes` 에 적혀 있음 |
| D-152 | 응답 시각은 UTC | ✅ 결정(D9-13) | 일 단위 필드(`event_date`, `kst_hour`)는 마트 정의대로 KST. 클라이언트가 변환 | Step 10 에서 KST 표시 |
| D-153 | `current` 는 `dim_link` 에 없는 link_id 의 관측을 내지 않는다 | ⏸ | 성능 때문에 링크 기준 LATERAL 로 읽는다(D9-12). 시드된 참조 링크에 없는 id 가 fact 에 있다면 누락된다(시험으로 고정) | `SELECT DISTINCT link_id FROM fact.traffic_speed t WHERE NOT EXISTS (SELECT 1 FROM dim.dim_link d WHERE d.link_id = t.link_id)` — 비어 있어야 정상 |
| D-154 | 장기 관측 수치가 합성 데이터 기반 | ❓ | 응답 시간은 링크 300·속도 150만 행·버스 67만 행 합성 데이터로 쟀다. 실제 링크 수·기간이 늘면 `traffic/current` 외의 `DISTINCT ON` 류(`bus/positions`)도 재측정 | Mac 에서 `time curl …`, 느리면 마트에 "최신" 표 추가(새 마이그레이션) |
| D-155 | 키셋 커서는 일관된 스냅샷이 아니다 | ✅ 설계 | 쪽을 넘기는 사이에 들어온 새 행은 앞쪽(정렬 기준 뒤)에서 보이지 않을 수 있고, 같은 정렬 키 중복은 보조 키로 처리한다. 시험: 정렬 키 동률에서도 끝까지 따라가면 빠짐·중복 없음 | — |
| D-156 | OpenAPI 계약과 라이브러리 경고 | ✅ · 🔧 | 스냅샷 시험이 계약 변경을 잡는다. Starlette 가 `httpx` 대신 `httpx2` 를 권하는 경고가 TestClient 에 나온다(동작 무관) | 다음 의존성 점검 때 `httpx2` 로 교체 검토 |
| D-157 | Compose 에서 `${API_KEYS:-}` (필수 `:?` 아님) | ✅ 설계 이탈 | 다른 서비스는 비밀을 `:?` 로 필수 처리하지만 `api` 만 그러면 `stack.sh ps/down/stats`(`--profile '*'`)가 API 키 없이 전부 실패한다. 대신 앱이 빈 키·비밀번호로 기동을 거부(fail-closed)하고 `up-api` 가 미리 안내. 시험: 정책 테스트·`docker compose config` | — |
| D-158 | `api_service` 비밀번호는 `db_bootstrap.py --api` 로만 | ✅ 설계 | 013 은 NOLOGIN 으로 만들어 안전한 쪽으로 실패. 역할 시험(`test_api_role.py`)은 클러스터 전체 역할을 지우므로 `ALLOW_ROLE_TESTS=1` 이 있을 때만 돈다(운영과 같은 클러스터에서 비밀번호를 지우는 사고 방지) | 런북 §6·§7 |
| D-159 | 부하 수치는 샌드박스(CPU 2개를 Postgres 와 공유) | ❓ | 141 rps · p95 192ms · 오류 0 은 동시 20개 기준. 대시보드 사용량(소수 동시 사용자)에는 충분하다고 [추정]. Mac 값은 다르다 | D-147 ⑤ |
| D-160 | 변이 시험 도구와 CI | ✅ · ✅ Step 11 (CI 초록불 확인 11-09) | `scripts/mutation_api.py` 30건(약 5분, Postgres 필요)은 수동 실행. CI 에는 `tests/api`(DB 없는 부분 + Postgres 서비스 컨테이너)와 `export_openapi.py --check` 를 넣는다 | Step 11 |


## J. Step 10 (대시보드 + Tableau) — 11-05

모든 항목은 **샌드박스(Python 3.12 venv · 가짜 API · 헤드리스 Chrome 화면 확인)에서만 검증**했다. 실제 API·DB 데이터, Docker 이미지 빌드·기동, Mac 실측은 확인 전이다. 결정: `docs/adr/0012-step10-dashboard.md`, 실행: `docs/runbooks/step10-dashboard.md`.
번호: 이 장은 D-165 부터. D-162·D-164 는 내용이 남아 있지 않아 2026-11-18 에 빈 자리 행을 정리했다(번호는 결번으로 둔다).

| ID | 항목 | 상태 | 현재 알려진 것 | 확인 방법 / 어디에 |
|---|---|---|---|---|
| D-161 | Tableau Public + API 기반 CSV(HANDOFF 기록) | ✅ 결정 → 옵션 A 구현 | 사용자 선택(옵션 A): 로컬 CSV 생성만, 게시는 사용자가 직접. `scripts/export_tableau.py` | ADR D10-8, `tableau/README.md` |
| D-163 | git push | 👤 | HANDOFF 기록대로(D-71 과 함께) 사용자 작업. `.gitignore` 로 `.env`·`data/`·`tableau/export/` 가 빠지는지는 시험으로 확인 | `git status` 로 `.env` 가 안 보이는지 확인 후 push |
| D-165 | Mac 에서 확인할 것(대시보드 7) | 👤 | ① `scripts/stack.sh init-env` 가 `API_KEYS` 끝에 `DASHBOARD_API_KEY` 를 덧붙이고 두 번째 실행은 "변경 없음" ② `scripts/stack.sh up-dashboard` 이미지 빌드(python:3.12-slim-bookworm, `streamlit==1.66.1` 휠)·healthy ③ **읽기 전용 파일시스템에서 Streamlit 기동**(`HOME=/tmp`, tmpfs `/tmp`) — 컨테이너로는 시험하지 못했다 ④ Compose v2.10.2 에서 `dashboard` 서비스 ⑤ `docker stats` 메모리 — 한도 `DASHBOARD_MEM_LIMIT` 기본 512m 는 **[추정]** ⑥ 실제 API 로 7개 화면 열기, 사이드바 "API 호출(최근 1분)" ≤12 ⑦ `python scripts/export_tableau.py --preview` | `docs/runbooks/step10-dashboard.md` §1~5 |
| D-176 | Mac 실측 발견: 버스 `route_id` 필수(11-06) | ✅ 수정 | 지도·버스 화면과 `export_tableau.py` 가 HTTP 422 `validation_error` 로 실패 → 원인은 API 가 `route_id` 를 필수로 요구하는데 대시보드가 안 보낸 것(내 버그). `DASHBOARD_BUS_ROUTE_ID`(기본 753번)로 수정, 가짜 API 가 스냅샷대로 검증하게 강화, 변이 3건 추가. **교훈:** 계약 시험이 필드 이름뿐 아니라 필수 매개변수도 대조해야 한다 | ADR D10-10 |
| D-177 | 노선이 1개로 고정 | ⏸ | API 에 노선 목록 엔드포인트가 없어 노선을 화면에서 고를 수 없다. 환경 변수로만 바꾼다 | 노선을 늘리면 `/v1` 에 목록 엔드포인트 추가 |
| D-178 | 날씨 효과 비율 1.0 (해결) | ✅ | 기준선(`mv_link_baseline`)이 비교 대상 날 자신을 포함해, 수집 기간이 짧으면 비율이 항상 1.0 이었다. `sufficient` 에 `기준선 n_days >= 2`(`WEATHER_MIN_BASELINE_DAYS`) 추가 → 지금은 날씨 화면이 "표본 부족"으로 보이는 게 정상. 기준선에서 비교 날을 빼는 것(leave-one-out)은 아님 | 수집 2주 이상 쌓이면 자연히 채워짐. 근본 개선은 비교 날 제외 기준선 |
| D-179 | 돌발 유형 코드표 일부만 앎 | ⏸ | 화면·CSV 가 A01 사고·A04 공사·작업·A10 행사·통제·A11 기타 4개만 한글로 보임. 그 밖의 코드는 "미확인 코드 (Axx)". CSV 는 코드(`acc_type`)와 한글(`acc_type_label`) 열을 둘 다 가짐 | 서울시 돌발 코드표 확인(api-catalog 의 `acc_type` 체크리스트와 동일) |
| D-180 | 2026 공휴일 시드는 웹 자료 기준 | ⏸ | `sql/014_holidays_2026.sql`(마이그레이션) + `dashboard/holidays.py` 가 같은 20일 목록(시험이 대조). **인사혁신처 고시와 대조 전 [미확인]**. 2027년 이후는 비어 있어 평일/토/일로 계산. 마이그레이션이 `is_holiday` 를 바꾸므로 `congestion_threshold` 는 다음 재계산부터 `day_type='holiday'` 행을 만들 수 있음 | 공공데이터 특일 정보 승인(D-74, D-101) 후 `reference_data_sync` 로 대체 |
| D-166 | 화면을 눈으로 본 것은 샌드박스 스크린샷뿐(→ 11-06 Mac 에서 개요·혼잡·돌발·날씨·이상·건강 실화면 캡처 확인, 지도·버스는 D-176 수정 후 재확인 필요) | ❓ | 가짜 데이터로 7개 화면을 헤드리스 Chrome 으로 찍어 레이아웃·글자 겹침을 확인했다. 실제 데이터(링크 4개·판정 불가 위주)에서의 모양, 모바일 폭, 다크 모드는 확인하지 않았다 | Mac 브라우저에서 확인, 어색하면 알려 주기 |
| D-167 | 지도 바탕 타일은 브라우저가 인터넷으로 받는다 | ⏸ | pydeck 의 carto 밝은 바탕. 오프라인이면 점·선만 보인다(샌드박스 스크린샷이 그 상태). 대시보드 서버는 외부로 나가지 않는다 | 오프라인 지도가 필요하면 단색 바탕 옵션 추가 |
| D-168 | 신선도(stale) 판정은 캐시 수명만큼 늦을 수 있다 | ✅ 설계 | 신선도는 API `meta.stale` 이 기준(null 이면 판정 안 함), 나이만 지금−as_of 로 다시 계산한다. 캐시(gold 5분·mart 15분) 때문에 stale 로 바뀌는 순간이 그만큼 늦다 | 더 빠르게 하려면 `DASHBOARD_TTL_SCALE` 을 줄인다(호출 예산과 맞바꿈) |
| D-169 | 요일 구분은 근사(공휴일 → 평일) | ⏸ | 시계열에 기준선을 붙일 때·내보내기의 `day_type_guess` 는 평일/토/일만 구분한다. API 는 날짜→day_type 표를 주지 않는다. 공휴일에는 기준선이 어긋날 수 있다(화면에 표시) | API 에 날짜별 day_type 을 추가하면 정확해진다(`/v1` 추가 필드) |
| D-170 | Tableau Public 게시 — 약관·공개 범위·원천 데이터 이용 조건 | ❓ 👤 | **확인하지 못했다.** Tableau Public 게시물은 누구나 볼 수 있고 비공개 옵션이 없다는 점은 알려져 있으나 현재 약관·한도는 직접 확인하지 않았다. 서울 열린데이터광장·공공데이터포털 이용 조건(출처 표시·재배포)도 미확인 | 사용자가 직접 확인 후 게시. 점검표는 `tableau/export/manifest.json` |
| D-171 | 버스 드리프트 CSV 는 시간 단위 그대로 | ✅ 설계 | 일 단위로 다시 집계하면 백분위(p50·p90)를 평균 내게 되어 틀린다 → 마트 값을 시간 단위 그대로 내보낸다 | ADR D10-8 |
| D-172 | 내보내기는 로컬 API 만 | ✅ 설계 | `127.0.0.1`/`localhost` 외 주소는 거부(키가 다른 곳으로 가지 않게) | — |
| D-173 | 대시보드 시험은 가짜 API 기반 | ⏸ 일부 해소(Step 11 e2e `tests/e2e/test_api_dashboard_pg.py`, 샌드박스 PG16) | 실제 Postgres 와 연결한 end-to-end 는 샌드박스에 PostGIS 가 없어 못 했다. 계약 시험이 대시보드가 읽는 필드를 OpenAPI 스냅샷과 대조해 어긋남은 막지만, 값의 의미는 Mac 에서 눈으로 확인해야 한다 | D-165 ⑥ |
| D-174 | 변이 시험 33건(원래 31이라 적었으나 실제 33)·CI | ✅ · ✅ Step 11 (CI 초록불 확인 11-09) | `scripts/mutation_dashboard.py` 는 Postgres·Docker 없이 약 15초. CI 에 `tests/dashboard`·`tests/unit` 과 함께 넣는다 | Step 11 |
| D-175 | 대시보드 의존성 고정 | ⏸ | `requirements-dashboard.txt` 는 샌드박스에서 시험한 정확한 버전(streamlit 1.66.1, pandas 3.1.1, pydeck 0.9.3, altair 6.3.0, requests 2.34.2). Streamlit 은 Python ≥3.11 필요. 올릴 때 `tests/dashboard` 통과 확인. `httpx2` 경고(D-156)는 그대로 | 다음 의존성 점검 |


## K. Step 11 (시험 계층 + GitHub Actions CI/CD) — 11-08

**워크플로는 샌드박스에서 실행할 수 없어서 GitHub 에서 한 번도 돌려 보지 못했다.** 확인한 것은 도구·시험·정적 점검뿐이다(814 passed / 3 skipped, 변이 `mutation_ci` 35 · `mutation_dashboard` 33 · `mutation_api` 30 전부 검출). 결정: `docs/adr/0013-step11-ci-cd.md`, 실행: `docs/runbooks/step11-ci.md`.
번호: 이 장은 D-183 부터.

| ID | 항목 | 상태 | 현재 알려진 것 | 확인 방법 / 어디에 |
|---|---|---|---|---|
| D-183 | 액션 SHA 고정 | ✅ 11-09 | 사용자가 `scripts/pin_actions.py` 실행(ci 16·release 2·weekly 10개 고정). 이후 zip 을 다시 풀면 워크플로가 `@vN` 태그로 되돌아가므로 **unzip 뒤 `python scripts/pin_actions.py` 를 다시 실행**(이미 고정된 줄은 건드리지 않는다). Dependabot 이 갱신 PR 을 만든다 | 런북 1단계 |
| D-184 | gitleaks 핀 | ✅ 11-09 | 사용자가 `scripts/pin_gitleaks.py` 실행 → 8.30.1 핀을 커밋. `secrets` job 은 첫 실행에서 통과 | — |
| D-185 | 실제 GitHub Actions 실행 | ✅ 11-09 | **`ci`·`weekly`·`release` 세 워크플로 모두 초록불**(사용자 확인). 첫 실행에서 나온 오류는 전부 수정: `dags`(Airflow constraints 와 fastapi 고정 충돌), `weekly` spark(`-m spark` 로 unit 파일을 걸러 0개 선택, exit 5), `weekly` 이력 스캔(`.env.bak` 8건 → D-203), `release`(새 패키지 첫 푸시 거부 → 재실행, D-205) | Actions 탭 |
| D-186 | CI/CD 범위 밖으로 둔 것 | ⏸ | ① Kafka→Spark→Gold 전체 e2e(비용 큼) ② Airflow·Spark 이미지는 CI 빌드·GHCR 게시 안 함 ③ arm64 이미지 없음(Mac 은 로컬 빌드) ④ Trivy/SBOM/이미지 서명 없음 ⑤ 배포(서버 없음) | 필요해질 때 ADR |
| D-187 | `api-contract-watch`(실제 공공 API 주기 호출로 응답 모양 변화 감시) | ⏸ | 하지 않음: 실제 키를 CI Secrets 에 넣어야 하고(결정 2 와 충돌), 호출 한도를 쓴다. 대안: 수집기가 이미 `rejects`·스키마 위반을 감사 로그에 남긴다 | 키를 CI 에 둘 의사가 생기면 별도 결정 |
| D-188 | Actions 사용량(분) | ✅ 11-09 | 실측: **Actions Linux 128분(Nov 7~9), $0.006/분, 총액 $0.77, 청구 $0**. 오늘 `ci`·`weekly`·`release` 를 여러 번(실패·재실행 포함) 돌린 값이라 평소 PR 비용은 이보다 작을 가능성이 크다 [추정]. 월 포함량 2,000분의 약 6.4%. `weekly` 1회 소요 분은 아직 모름 | Billing → Usage |
| D-189 | CI 첫 초록불로 닫을 수 있는 것 | ✅ 11-09 | `test-pg` 초록 → PG17/PostGIS 3.6.1 에서의 시험 항목 닫음, D-160·D-174(CI 반영) 닫음. D-191(amd64 PostGIS 이미지)도 서비스 컨테이너가 떠서 해소 | — |
| D-190 | Spark 컨테이너 시험이 새 `pytest.ini` 로 도는지 | ✅ 11-09 | 이미지 재빌드 후 `scripts/spark_test.sh` **114 passed / 4 skipped**(Postgres 시험은 컨테이너에 DB 가 없어 skip). 처음 두 번 실패: ① 컨테이너 pytest 는 시스템 패키지(구버전, Python 3.10)라 `-p layers_plugin` 을 PYTHONPATH 적용 전에 import 하다 실패 ② `pytest.ini` 의 `pythonpath` 를 무시해 `pg_support` import 실패(②는 Step 11 이전부터 있던 잠복 문제로 [추정] — 두 파일이 컨테이너에서 돈 기록이 없다). 래퍼 `docker/spark-base/urbanstream-pytest.sh` 가 `tests/support` 를 PYTHONPATH 에 넣고 `-o addopts=-ra` 로 덮어쓰도록 수정, 정적 시험·변이 2건 추가 | 런북 |
| D-191 | PostGIS 이미지의 linux/amd64 매니페스트 | ✅ 11-09 | `test-pg` 서비스 컨테이너가 러너(amd64)에서 기동·시험 통과 | — |
| D-192 | 실제 docker 빌드·이미지 검사(`image-guard`)·GHCR 푸시 | ✅ 11-09 | `ci` 의 `docker-build` 초록, `release` 로 `urbanstream-api`·`urbanstream-dashboard` 가 GHCR 에 올라감(태그 `0.1.0`·`sha-<7>`, private). 새 패키지 첫 푸시는 한 번 거부된다(D-205) | — |
| D-193 | 브랜치 보호 | 👤 ⏸ | 선택. 필수 체크는 `ci-ok` 하나. private Free 플랜에서의 지원 여부 [미확인] | 런북 5단계 |
| D-194 | 저장소를 public 으로 바꿀지 | ❓ 👤 | 분은 무제한이 되지만 코드·이력이 공개된다. 먼저 주간 `secrets-full`(전체 이력 스캔) 통과와 D-71(데이터 파일 이력) 확인 필요 | 포트폴리오 공개 단계 |
| D-195 | `.env.bak` 이 커밋 71b5977 로 push 됨(11-09) | ✅ 공개 전 처리 | Step 11 의 `tracked` 시험이 발견했다. 외부 키 3개(서울 열린데이터광장·data.go.kr·Discord 웹훅)를 재발급해 노출된 값을 폐기했고, `git filter-repo` 로 `.env.bak` 과 내부 작업 문서·과거에 추적된 zip 을 전 이력에서 제거했다. 이력 전체 gitleaks 검사에서 발견 0건을 확인한 뒤 `.gitleaks.toml` 의 `.env.bak` 허용과 `.gitleaksignore` 지문을 지웠다. GitHub 이 옛 커밋을 SHA 로 한동안 보관할 수 있어 키 폐기가 실제 방어다 | — |
| D-203 | `.gitleaksignore`(지문 8개)와 `.gitleaks.toml` 의 `.env.bak` 허용 | 👤 ⏸ | 11-09 `weekly` 의 전체 이력 스캔이 `.env.bak` 값 8개를 정확히 찾아냈다(스캐너 동작 확인). 경로 허용이 이력 스캔에서 먹히지 않아 지문 파일로 무시. **공개 전환 전에 두 허용을 모두 제거하고 키를 폐기** (D-195) | D-195 |
| D-204 | 11-09 `weekly` 첫 실행에서 나온 워크플로 오류 2건 | ✅ | ① `spark` job 이 `-m spark` 로 `tests/unit` 파일을 걸러 0개 선택(exit 5) → 경로 직접 지정으로 수정 ② `secrets-full` 이 위 8건 지적. 둘 다 시험·변이 시험 추가 | — |


## L. Step 12 (Azure 클라우드 배포) — 계획 단계, 11-09

아직 구현 전이다. 결정 제안서는 새 채팅에서 쓰고 사용자가 승인한 뒤 구현한다(HANDOFF 7-12). 번호는 D-196 부터.

| ID | 항목 | 상태 | 현재 알려진 것 | 확인 방법 / 어디에 |
|---|---|---|---|---|
| D-196 | 클라우드 IP 에서 한국 공공 API 호출이 되는가 | ❓ 🔴 | **확인하지 못했다.** 해외·클라우드 IP 를 막는 API 가 있을 수 있다 [추정]. 막히면 이 단계의 전제가 무너지므로 가장 먼저 확인 | VM 을 만든 직후 `collector once` 로 5개 소스 호출 |
| D-197 | Azure 크레딧 금액·만료일 | ✅ 확인(11-11): ₩101,300/₩133,765, 2027-08-15 만료, D-229 | 11-11 `az login`·`az account show`: 구독 **`Azure for Students`**(`Enabled`, 테넌트 가톨릭대학교). 이 프로그램은 **$100 크레딧·12개월**이고 크레딧을 다 쓰거나 12개월이 지나면 구독이 비활성화된다고 안내됨(Microsoft 안내·대학 안내 페이지) — **사용자 포털의 실제 잔액·만료일은 아직 못 받음**. 허용 리전·쿼터는 D-225 | 새 채팅 첫 질문 |
| D-198 | 도메인·TLS | ❓ 👤 | 도메인 보유 여부 모름. 없으면 Azure 가 주는 DNS 이름(`<이름>.koreacentral.cloudapp.azure.com`)으로 인증서를 받을 수 있다고 [추정] | 새 채팅에서 질문 |
| D-199 | 대시보드(Streamlit)에 로그인이 없다 | 🔴 ⏸ | API 는 `X-API-Key` 가 있지만 대시보드 화면 자체는 인증이 없다. 공개 주소로 열려면 프록시 단 인증(basic auth) 또는 IP 허용이 필수. 로컬(127.0.0.1)에서는 문제 없었다 | Step 12 설계에 포함 |
| D-200 | Mac 수집기와 VM 수집기를 동시에 돌리면 API 호출 한도를 두 배로 쓴다 | ⏸ | 버스 호출 한도가 아직 미해결이다(HANDOFF 9장). 전환 시 Mac 수집기를 먼저 중지 | 전환 절차서에 명시 |
| D-201 | VM 크기 근거 | ⏳ 1차 측정(11-10, D-216) | 스트리밍만 가동했을 때 합계 약 1.0 GiB(11-03)였으나, Airflow 3개·API·대시보드·monitor 까지 전부 켠 합계는 측정한 적이 없다(D-90). Docker VM 7,851 MiB 에서 돌고 있음 | Mac 에서 전체 가동 후 `scripts/stack.sh stats` 합계 |
| D-202 | 신규 DB 에 마이그레이션 001~014 를 처음부터 적용하는 경로 | ✅ 2026-11-17 (간접) | CI 의 `test-pg` 와 샌드박스에서는 시험 fixture 로 돌지만 `scripts/migrate.py` 로 빈 DB 에 순서대로 적용하는 경로를 클라우드에서 처음 실행한다. 이미 적용된 마이그레이션은 수정하지 않는다 | Step 12 구현 시 **11-17: VM 에서 `cloud.sh up` 이 빈 DB 에 마이그레이션을 적용하고 스냅샷을 복원해 `/ready` 가 `db: ok`(증거 09)** — 사용자 보고·캡처 기준, 마이그레이션 로그는 못 봤다 [미확인] |

## M. Step 11 마감에서 나온 것 + Step 12 설계 입력 (11-09 오후)

| ID | 항목 | 상태 | 현재 알려진 것 | 확인 방법 / 어디에 |
|---|---|---|---|---|
| D-205 | GHCR 새 패키지 첫 푸시가 `denied` | ✅ 우회 | `release` 에서 레이어는 올라가고 마지막 단계만 `error from registry: denied`. 토큰은 `Packages: write` 였고 패키지 Actions 접근도 admin 이었다. **재실행하면 통과**(api, 이어서 dashboard 모두). 새 패키지가 처음 생길 때 저장소 접근 연결이 늦는 것으로 [추정] | 새 이미지를 게시할 때(예: Airflow·Spark) 첫 푸시는 재실행. 필요하면 `release.yml` 에 푸시 재시도 추가 |
| D-206 | **수집기(collector)는 compose 서비스가 아니다** | 🔴 ⏸ Step 12 | `docker-compose.yml` 에 collector 가 없다. 지금은 Mac 호스트에서 `python -m collector run`(`caffeinate -i`)으로 돌고 Kafka 에 `localhost:9092`(`KAFKA_BOOTSTRAP`)로 붙는다. 상태는 `data/collector/budget.json`. 클라우드에서는 이것을 어떻게 24시간 돌릴지(컨테이너화 + override 서비스 vs systemd) **먼저 결정해야 한다** | Step 12 결정 제안서 D12-13 |
| D-207 | 호스트 Python 스크립트에 기대는 기동 절차 | ⏸ Step 12 | `stack.sh up-*` 가 호스트의 `python3` 로 `scripts/migrate.py`·`scripts/db_bootstrap.py`·`scripts/stack_env.py` 를 실행한다(psycopg 필요). 수동 Gold 실행(`scripts/gold_run.sh`)도 Spark 는 Parquet 만 만들고 DB 적재는 호스트 Python(`batch/gold_load.py`)이 한다(ADR-0008 ⑤; Airflow 경로는 컨테이너에서 실행 — 확인 필요). VM 에도 같은 Python 환경(3.11+)이나 이를 대신하는 컨테이너 경로가 필요 | Step 12 구현 시 |
| D-208 | compose 이미지 일부가 고정되지 않음 | ⏸ | `kafbat/kafka-ui:latest`(dev 프로파일만). 운영 스택은 `apache/kafka:3.9.2`, `imresamu/postgis:17-3.6.1-bookworm`, `urbanstream/spark-bronze:4.1.3`, `urbanstream/airflow:3.3.0` 으로 고정 | 클라우드 VM 에는 dev 프로파일을 올리지 않는다 |
| D-209 | Mac 의 Compose 는 v2.10.2(오래됨) | ⏸ | `stack.sh` 가 `--wait-timeout` 을 쓰지 않고 `airflow-scheduler` 를 먼저 따로 빌드하는 등 v2.10 의 한계를 우회한다. VM 의 Compose 는 더 새로울 수 있어, 우회 코드가 그대로 동작하는지 확인 필요 | Step 12 첫 기동 |
| D-210 | Airflow 이미지의 pytest 가 구버전일 수 있다 | ❓ | Spark 이미지는 시스템 pytest(구버전)라 `pytest.ini` 의 `pythonpath`·`-p` 를 못 쓴다는 것이 확인됐다. Airflow 이미지에서 시험을 돌린 적은 없다 — 돌릴 일이 생기면 같은 점검 | 필요 시 |

## N. Airflow OOM 진단 + Step 12 설계 입력 (11-10)

Mac 에서 Airflow 컨테이너 2개가 죽어 있는 것을 `stack.sh ps`(exited 137)로 발견했고 `docker inspect` 로 둘 다 `OOMKilled=true` 를 확인했다. 시각은 로그가 UTC, 아래는 KST(+9h).

| ID | 항목 | 상태 | 현재 알려진 것 | 확인 방법 / 어디에 |
|---|---|---|---|---|
| D-211 | **Airflow scheduler·dag-processor OOM 종료** | 🔴 ✅ 재기동 / ⏸ 구조 해법 | dag-processor(한도 768m): 11-07 15:19 KST 종료. scheduler(한도 3g): 11-09 11:41 KST 종료. 11-10 에 `.env` 에 `AIRFLOW_DAGPROC_MEM_LIMIT=1g`·`AIRFLOW_SCHEDULER_MEM_LIMIT=4g`(사용자 로컬, **저장소 기본값 768m/3g 는 그대로**)를 넣고 `stack.sh up-airflow` 로 재기동. **임시값이다 — 최종 값은 Step 12 에서 실측 후**. **근본 원인은 한도 부족이 아니라 Mac 덮개 닫힘·절전 주기(D-222)로 보인다 — 한도 상향은 완충일 뿐** | `scripts/stack.sh stats`, `docker inspect -f '{{.State.OOMKilled}}' <컨테이너>` |
| D-212 | **고아 판정 → 같은 작업의 중복 실행 → OOM** (원인 연쇄) | 🔴 ⏸ Step 12 | scheduler 로그(11-09 02:23~02:41 UTC): ① heartbeat 가 102·108·118·362 초 뒤에야 복구(정상 5초 안팎) ② 그 사이 실행 중이던 `gold_intraday.run` 을 "고아 작업"으로 reset → 2번째 시도 시작 ③ 1번째 시도 프로세스(pid 34239)가 SIGTERM·SIGKILL 에도 안 죽음(`Failed to terminate process after full escalation`) ④ 같은 컨테이너에 Gold 실행이 2개 동시에 존재. 단일 Gold 실행 ≈ 1.6 GiB(11-10 `stats`: 1.61 GiB) × 2 ≈ 3.2 GiB > 3g → 02:41:14 UTC OOM. 02:40 에 2번째 시도도 같은 방식으로 고아 처리됨. **2개 동시 메모리는 직접 재지 않았다 [추정]**. 이미 `max_active_runs=1`·`execution_timeout` 45분·풀 `gold_io` 가 있었지만 같은 실행의 재시도 겹침은 막지 못한다(정정: 처음 제안한 "이 둘을 추가"는 이미 있어 해법이 아니다). **11-09 시도 2 로그(사용자 제공, KST):** 11:28:51 `gold_stage 시작` 뒤 12분간 로그 없음 → 11:40:46 고아 reset 으로 `Task killed`(410 not_found) → 11:41:14 OOM. Spark 가 도는 동안 scheduler heartbeat 가 끊긴 것으로 보인다 [추정] → **heartbeat 단절의 원인은 Mac 덮개 닫힘 절전 주기로 확인됨(D-222)**: 10:38:27 덮개 닫힘(Clamshell Sleep) 뒤 약 1분 절전/45초 기상을 반복, 11:39:54 에 마지막으로 깨어난 지 약 1분 뒤 고아 reset(11:40:41)·Task killed·OOM(11:41:14) | 해법 후보: Gold 의 Spark 를 scheduler 안이 아니라 별도 일회성 컨테이너(`gold_run.sh` 방식)로 분리 / 컨테이너 한도를 중복 실행분까지 포함해 산정. Step 12 결정 제안서 D12-15 |
| D-213 | **DAG 파일 파싱이 너무 느림 → DAG 비활성화** | 🔴 ⏸ Step 12 | dag-processor 로그(11-07): `anomaly_scan.py` 33~50초, `silver_reconcile_daily.py` 50초(파싱 제한 50초 초과로 kill), `incident_impact_finalize.py` 46초, `weather_traffic_analysis.py` 42초, `gold_intraday.py` 31초, 나머지 2~24초(정상은 1초 안팎). 제한을 넘긴 2개는 "Deactivating stale DAG" 로 **스케줄에서 빠졌다**. 작업 시작 때도 파싱 30초 + Pre Execute 약 2분. 원인은 확정 못 함 — CPU 경합(4 vCPU 공유, spark-silver 110%)·Mac 바인드 마운트의 느린 import·DAG 파일 최상위의 무거운 import 가 후보 [추정] | 확인: DAG 파일의 최상위 import 를 함수 안으로 옮긴 뒤 파싱 시간 재측정(`airflow dags report`). 클라우드 Linux(바인드 마운트 아님)에서는 달라질 수 있다 |
| D-214 | 재시작 정책 `on-failure` 가 두 컨테이너를 되살리지 못함 | ❓ 🔴 | 왜 자동으로 안 켜졌는지 **모른다**(Docker 재시작·수동 중지 후엔 안 켜진다고 알려져 있으나 이 경우는 미확인 [추정]). 11-10 15:20 확인 결과 재시작횟수 0·정책 on-failure 이지만 **이 컨테이너들은 `up-airflow` 로 이미 재생성된 뒤라 과거 정보가 아니다 — 원인은 영영 확인 못 할 수 있다.** 다음에 죽으면 재생성 **전에** `docker inspect` 로 `RestartCount`·`State.Error`·`Finished` 를 먼저 남긴다 | Step 12 override(`docker-compose.cloud.yml`)에서 `restart: unless-stopped` + 헬스체크 기반 재시작 점검(D12-15) |
| D-215 | **11-07~11-09 Airflow 공백의 데이터 영향** | ⏳ 알림은 확인(11-10), 마트 신선도는 미확인 👤 | dag-processor 가 11-07 15:19 부터, scheduler 가 11-09 11:41 부터 죽어 있었으므로 Gold·마트 갱신이 멈췄을 수 있다. 대시보드 마트 신선도(stale 여부), Discord 알림이 이 기간에 왔는지 **사용자 확인 대기**. **Discord 기록(11-10 사용자 제공):** 컨테이너 종료·OOM 자체를 알리는 메시지는 없었다(제공된 기록 기준). 대신 Airflow 작업 실패 알림은 왔다: 11-07 아침 요약에 `gold_intraday.run ×2`·`anomaly_scan.check_upstream`, 11-10 14:57 에 `gold_intraday.run` **42건 묶음**(대표 run_id `scheduled__2026-11-07T01:20`)·`anomaly_scan.check_upstream`·`dq_report_daily.report`(`scheduled__2026-11-08T20:00`). 이 묶음이 재기동 직후 한꺼번에 도착했고 11-09 에 `(이전 전달 실패 1건)` 이 있었다 → 알림 전달이 밀렸거나 유실됐을 수 있다 [추정, 경로 미확인]. 즉 **Gold 는 11-07 낮부터 계속 실패 중이었고(D-219) 그 사실이 이틀 넘게 눈에 안 띄었다** | 대시보드 http://127.0.0.1:8501 의 건강 화면, `GET /v1/pipeline/health` |
| D-216 | 전체 가동 실측(11-10 15:01 KST, Airflow 재기동 후) | ✅ 측정 | Docker VM 10,957 MiB / CPU 4개. 합계 약 5.2 GiB: scheduler 1.61 GiB(한도 4g, Gold 1건 진행 중·CPU 53%), spark-silver 1.217 GiB(한도 1.5g의 81%, CPU 110%), spark-bronze 847 MiB, kafka 535 MiB, apiserver 298 MiB, postgres 242 MiB, dag-processor 219 MiB, dashboard 128 MiB, api 51 MiB, monitor 36 MiB. 파일 `data/stack/stats_20261110T150111.txt`. **spark-silver 가 한도에 가장 가깝다**(D-07 83%, 11-09 75%) — 다음 OOM 후보 | VM 크기 근거(D-201 갱신): Gold 중복 실행(≈+1.6 GiB)과 CPU 경합을 고려해 RAM 16 GiB급 + vCPU 여유 [추정] |
| D-217 | Mac 에 `az`·`terraform` 없음 | 👤 ⏸ | 11-10 확인: 둘 다 없음. 설치: `brew install azure-cli`, `brew tap hashicorp/tap` → `brew install hashicorp/tap/terraform`. 그 뒤 `az login`, `az account list -o table`, `az vm list-usage --location koreacentral -o table` 결과를 새 채팅에 | D-197 과 함께 |
| D-218 | 전달용 zip 이 저장소에 추적됨 | ✅ | `git add -A` 로 `urbanstream_full.zip` 이 추적된 적이 있어 사용자가 삭제를 스테이징. `.gitignore` 에 `*.zip` 추가, 런북에 `rm` 단계 추가, 시험 `test_delivery_zip_is_ignored` | — |
| D-219 | **`gold_intraday.run` 이 OOM 이전부터 계속 실패** | 🔴 ✅ 원인 확인(Mac 절전, D-222) | Discord 에 같은 종류 42건(11-07 01:20 UTC 실행분부터). 매시 20분 DAG 이므로 이틀 가까이 연속 실패했을 수 있다 [추정]. 실패 사유는 알림 본문이 비어 있어(`try=1 : `) 모른다. 후보: 고아 reset·execution_timeout(45분)·CPU 경합·메모리(D-212). **Gold 마트(`gold_*`)가 11-07 낮 이후 갱신되지 않았을 가능성**. **표본 1건 확인(11-10, `scheduled__2026-11-07T01:20` attempt=1.log, UTC):** 로그 이벤트가 4개뿐 — `Pre Execute` 가 05:42:41 에 시작(실행 예정 시각보다 수 시간 뒤; 풀 `gold_io` 대기나 VM 정지 [추정]), 다음 줄은 06:19:16(37분 공백), 06:19:49 에 SIGTERM(`Process terminated by signal`). **`gold_stage` 시작 줄이 없다 = Spark 단계에 도달하지 못했다.** 즉 이 연속 실패는 Spark 메모리 부족이 아니라 스케줄링·인프라 쪽(D-213 파싱 지연, 05:19~05:42 의 23분 heartbeat 단절, 06:19:29 dag-processor OOM 20초 뒤 SIGTERM)이다 [추정]. 그 시각에 Mac 이 절전이었는지가 핵심 가설 → **11-10 `pmset -g log` 로 확인됨(D-222)**: 05:19~05:42 UTC heartbeat 단절 = Mac 절전 구간, 06:19:49 SIGTERM = Mac 이 다시 잠드는 순간과 같은 초 | (검증 완료 — D-222.) 11-09 구간도 확인함(D-222) — 명령: `pmset -g log \| grep -E '^2026-11-09 1[0-2]:' \| grep -E ' (Sleep\|Wake) '`. (이전 가설 검증 명령) Mac 절전 기록 `pmset -g log \| grep -E ' Sleep | Wake '` 에서 11-07 14:19~15:20 KST 와 비교. 절전이 맞으면 클라우드(24시간 켜진 VM)에서는 이 종류가 사라지지만, 깨어난 직후 DAG 파싱이 한꺼번에 몰려 dag-processor 한도(D-211)를 넘기는 문제는 남는다. `gold_intraday` 풀 슬롯(`gold_io`) 점유도 확인 |
| D-220 | 아침 요약의 `소스 이상: 5개 소스 전부 unknown` 이 매일 반복 | ❓ 절전과 관련 가능(D-222) [추정] | 11-07~11-10 아침 요약 4번 모두 5개 소스가 `unknown`. 수집기가 도는 시간대(Mac 절전 가능)와 요약 시각의 관계, 또는 상태 계산 로직 문제인지 [미확인]. 포트폴리오 증거가 되는 "소스 상태"가 신뢰를 잃는다 | Step 8 `monitor` 의 `unknown` 판정 조건 확인(HANDOFF Step 8)·실제 수집기 가동 시각과 대조 |
| D-221 | 알림이 "증상"만 알리고 "컨테이너 종료"는 안 알림 | 🔴 ⏸ Step 12 | 11-07 15:19·11-09 11:41 에 Airflow 컨테이너가 죽었지만 그것을 알린 메시지가 없다. 반대로 Bronze/Silver 적재 정체·소스 down 알림은 정상 동작(11-09 03:01 `bronze seoul_incident` 4시간 정체 → 10:10 해소 등). 버스 소스는 11-10 12:18 `down`(호출 7회/실패 7회, 마지막 성공 11:09 KST)·12:52 재발 — 버스 호출 한도 미해결(D-200·HANDOFF 9장)과 관련 [추정] | D12-16 에서 monitor 에 컨테이너 상태 점검 추가 |
| D-222 | **Mac 절전으로 11-07 에 수집·처리가 사실상 멈춰 있었다** | 🔴 ✅ 확인 / ⏸ 대응은 Step 12 | `pmset -g log` 11-07 00:16~23:55 KST: **절전 진입 115회, 깨어 있던 시간 합계 약 37분, 잠든 시간 약 1,382분(≈23시간, 97%)**. 배터리(`Using Batt`, 충전 55%)에서 약 15분 잠들고 몇 초~45초 깨는 'Maintenance Sleep/Sleep Service' 주기가 하루 종일 반복(최대 연속 절전 18분). Docker VM·수집기·Kafka·Spark·Airflow 가 그 사이 멈춘다 [추정: VM 일시정지는 직접 확인 안 함, 로그 시각이 일치]. **일치하는 증거:** ① dag-processor `Heartbeat recovered after 1391 s`(05:42 UTC=14:42 KST) = 절전 14:18:49~깨어남 14:41:40(1,371초)과 20초 차이 ② 같은 작업 로그의 06:19:49 UTC `SIGTERM` = Mac 절전 진입 15:19:49 KST 와 **같은 초**, dag-processor OOM(15:19:29)은 15:19:04~15:19:49 의 45초 깨어난 창 안 ③ 매일 아침 요약의 `5개 소스 unknown`, 야간 `no_new_file_14958s`(4시간) 적재 정체 알림도 같은 부류로 보인다 [추정]. **11-09 도 확인(11-10 `pmset`):** 10:38:27 KST 덮개 닫힘(`Clamshell Sleep`, 배터리 44%→38%) 뒤 11:39:54 까지 약 1분 절전/45초 기상 주기(40회)를 반복, 같은 구간의 heartbeat 지연(102~362초)·고아 reset·OOM(11:41:14)이 모두 그 안에 있고 OOM 은 마지막 기상(11:39:54) 약 80초 뒤. 11-09 OOM 도 절전이 방아쇠로 [추정]. **결과:** 11-07 의 수집 데이터는 거의 비어 있을 수 있고, Mac 에서 모은 수치(수집 일수·레코드 수)는 포트폴리오 증거로 부적합 — 클라우드 24시간 가동 데이터로 대체 | Bronze 일별 파일 수로 11-07 결측 확인(`data/bronze`). Mac 에서 계속 돌릴 거면 전원 연결 + 덮개 열어두기 + `caffeinate -i`(잠자기 방지는 덮개를 닫은 배터리 상태에서는 소용없다). **근본 해법 = Step 12(24시간 켜진 VM)**. Step 12 전환 시 Mac 의 기존 `data/` 는 옮기지 않는다(D12-10) |
| D-223 | Mac 끄기·켜기 절차서 / 편의 명령 | ✅ 문서 / ⏸ 명령은 선택 | `docs/runbooks/mac-on-off.md` 추가(11-10). 켜는 순서가 4개 명령이라 `stack.sh up-all` 같은 편의 명령을 만들 수 있으나, `stack.sh` 의 Compose v2.10.2 우회 코드(D-209)와 기존 시험·변이를 건드리므로 **Step 12 에서 클라우드 기동 절차와 함께 결정**(지금은 만들지 않음) | Step 12 D12-7·D12-14 |
| D-224 | **예산: 학생 구독 크레딧(₩133,765, 잔액 ₩101,300)으로 24시간 가동은 약 2~3주** | ✅ 사용자 결정(11-11 오후): 상시 가동을 포기하고 '최소 증거 배포'(HANDOFF 7-12 맨 위) | `Standard_B4s_v2`(4 vCPU/16 GiB) 소매가 ≈ $0.166/시간(East US, Linux) ≈ $121/월 — **Korea Central 가격은 미확인**(포털 가격표·Azure Retail Prices API 로 확인). $100 이면 컴퓨트만으로 약 25일, 디스크·공용 IP·백업 포함 시 약 3주 안팎 [추정]. 크레딧 소진 시 구독이 비활성화되면 VM 이 멈춘다. **완료 기준 \"24시간 무중단 후 health ok\"는 가능하나 \"수주~수개월 연속 수집\" 포트폴리오 수치는 이 예산으로 불가** → 선택지: (A) 4/16 으로 약 3주 가동 후 `terraform destroy`(코드·스크린샷·수치 보존) (B) 2 vCPU/8 GiB(`B2s_v2`) 로 약 2배 오래 — 메모리·CPU 여유 부족으로 Mac 의 OOM 문제 재현 위험 [추정] (C) 사용자가 종량제(카드)로 전환 — 사용자 결정 (D) 컨테이너 구성 축소(대시보드·감시 제외 등)로 8 GiB 시도. **중지(deallocate)하면 컴퓨트 요금은 멈추지만 디스크 요금은 계속 + 수집 공백이 생긴다** | Step 12 결정 제안서에서 사용자와 선택. 포털 Cost Management 에 예산 알림(예: 크레딧의 50·80%) 필수(D12-9) |
| D-225 | **VM 크기·리전 가용성 (학생 구독)** | ⏳ 부분 확인(11-11) | `az vm list-skus --location koreacentral --size Standard_B`: **Korea Central 에 `Standard_B*_v2` 계열(2·4·8·16·32 vCPU, 영역 1·2·3)이 있고 `Restrictions` 모두 `None`** → 이 구독에서 리전·크기 제한은 보이지 않음. v1 B 계열(`B4ms` 등)은 목록에 없음. 4 vCPU/16 GiB 후보: `Standard_B4s_v2`(Intel), `Standard_B4as_v2`(AMD). 8 GiB: `B2s_v2`(2 vCPU). **`az vm list-usage` 결과는 받지 못함**(vCPU 쿼터 미확인). 포럼(2026-06)에는 학생 구독의 **4 vCPU 쿼터**·허용 리전 정책·용량 불일치 보고가 있음 → 4 vCPU 가 쿼터 한도일 수 있어 [추정] 한 대에 4 vCPU 이상은 불가. B 계열은 **버스터블**(기준 CPU 이하만 상시 사용 가능)이라 Spark 상시 부하에서 CPU 크레딧이 줄면 느려질 수 있다 [추정, 확인 필요] | 포털 Subscriptions → Usage + quotas(리전 vCPU·VM 패밀리), Policy → Assignments(\"Allowed resource deployment regions\"). 사용자 CLI: `az vm list-usage --location koreacentral -o table` 재시도 |
| D-226 | 학교 테넌트에서 OIDC 앱 등록 권한 | ❓ 👤 | GitHub Actions 가 비밀 없이 Azure 에 로그인하는 OIDC(D12-6)는 Entra ID 에 앱 등록·페더레이션 자격이 필요하다. 학교(가톨릭대학교) 테넌트는 일반 사용자의 앱 등록을 막았을 수 있다 [추정, 미확인]. 막혔다면 대안: 배포는 Mac 에서 `terraform apply`·`ssh` 스크립트로 수동 실행(Actions 는 시험·빌드만), 또는 SSH 키를 Actions 비밀로(Step 11 의 \"장기 비밀 없음\" 원칙 예외) | Step 12 초기에 포털 Entra ID → App registrations 에서 등록 가능 여부 확인 |
| D-227 | **구독에 첫 프로젝트(Olist) 잔존 리소스가 비용을 내고 있다** | 👤 결정 필요 | 11-11 포털 구독 개요: 리소스별 비용에 `ecommerce-app-plan`(App Service 플랜, `serverfarms` 1개), 리소스 종류에 `flexibleservers` 1개(PostgreSQL 유연 서버 [추정]). **현재 비용 ₩5.75K, 이달 예측 ₩17.40K**(통화는 원). 첫 프로젝트 데모라면 의도된 것이고(Azure 사용 경험의 증거이기도 하다), 아니라면 크레딧을 갉아먹는다. 삭제는 사용자 결정(삭제하면 그 데모가 내려간다) | 포털 구독 → 리소스 / 비용 관리. Step 12 시작 때 예산 알림을 만든다(D12-9) |
| D-228 | **vCPU 쿼터(Korea Central, 11-11 `az vm list-usage`)** | ✅ 측정 | `Total Regional vCPUs` 한도 **6**(사용 0), `Total Regional Low-priority vCPUs` 3, `Standard Bsv2 Family` 10·`Basv2` 10·`Bpsv2` 10, `Standard BS Family` 4, `D/Dv2/Dv3/DSv3/Dv4/DSv4` 계열 4, `Dv5`·`DSv5`·`Dasv6`·`Dsv6` 등 최신 세대는 **0**. 즉 **한 시점에 최대 6 vCPU**: `B4s_v2`(4 vCPU/16 GiB) 한 대 + 2 vCPU 한 대까지 가능, 8 vCPU 이상 불가. `Dedicated vCPUs` 0, `Virtual Machines` 25000. 최신 D 계열은 못 씀 → B v2 계열 또는 구세대 D | Terraform 변수 `vm_size` 기본값은 `Standard_B2s_v2`(2 vCPU/8 GiB) [제안] |
| D-229 | 크레딧 잔액 | ✅ 확인(11-11 15시, Education Hub '학생 제안 세부 정보') | 11-11 Azure Sponsorships 포털이 \"This account does not have an active Sponsorship.\" 를 보여줌(사용한 계정이 구독과 다른 계정이거나 이 학생 구독이 Sponsorship 방식이 아닐 수 있다 [추정]). 구독은 `Enabled` 이고 비용이 쌓이는 중이라 크레딧이 소진·만료된 상태는 아닌 것으로 보인다 [추정]. **[확정 11-11 15시 포털 화면] 사용 가능한 크레딧 ₩101,300 / 부여 ₩133,765(약 76% 남음), 만료 2027-08-15(남은 277일), 11월 비용 ₩6,368.** 즉 사용액 ₩32,465. 부여액이 $100 이라면 환율은 약 ₩1,338/$ 이고 잔액은 약 $76 [추정] — 아래의 \"약 $33 사용·잔액 $67\" 는 사용자 구두 보고라 **포털 값(₩)이 우선**한다. Sponsorships 포털은 \"활성 후원 없음\" 이라 쓰지 않는다. [이전 기록] **잔액·만료일은 포털에서 미확인이었다.** 사용자 보고(11-11): 학생 구독은 올해(2026) 가입, 올해 사용량 약 $33 → $100 기준 잔액 약 $67 [추정, 크레딧이 정확히 $100 인지·사용량에 Olist 가 포함되는지 미확인]. 만료는 가입일 + 12개월(가입일 미확인). 이 잔액이면 `B4s_v2` 상시 가동은 약 2주도 못 버티고(D-224), 최소 증거 배포(`B2s_v2` 수일, 수 달러 [추정])는 충분하다. Olist 잔존 비용(월 약 $12 [추정, ₩17.4K])이 같은 크레딧에서 빠진다(D-227) 카드가 연결돼 있지 않은지 확인 필요 | 포털 검색 `Education`(Education Hub 개요) 또는 비용 관리 + 청구 → 크레딧. 못 찾으면 Step 12 에서 예산 알림(비용 기준)만 둔다 |

## O. Step 12 구현 결과 + 남은 확인 (11-12)

Step 12 코드는 완료(샌드박스: 정적·단위 시험 + 변이)했고 **Azure 에서는 한 번도 실행하지 못했다.** 결정: `docs/adr/0014-step12-azure-evidence-deploy.md`, 실행: `docs/runbooks/step12-cloud.md`. L장 항목 중 D-198(도메인 불필요)·D-199(공개 안 함, SSH 터널)·D-200(Mac 수집기 중지 불필요)·D-201(VM 크기: 서빙 계층만 올리므로 B2s_v2)은 이 범위에서 해소/무효가 됐다. D-196(클라우드 IP 에서 공공 API)은 이번 범위 밖(키를 VM 에 넣을 때만). D-206(수집기 컨테이너화)·D-211~D-222 Mac 쪽 교훈은 그대로 남는다.

| ID | 항목 | 상태 | 현재 알려진 것 | 확인 방법 / 어디에 |
|---|---|---|---|---|
| D-230 | `terraform init/plan/apply`, VM 생성, `Standard_B2s_v2`+Ubuntu 24.04(Gen2) 조합 | ❓ 👤 Azure | 샌드박스에는 terraform 바이너리가 없다 — `.tf` 는 `python-hcl2` 로 구문만 확인했고 `terraform validate` 는 CI 첫 실행이 처음이다. azurerm `~> 4.0` 에서 속성 이름이 틀렸을 가능성 [추정] | 런북 3절. 오류 문구를 새 채팅에 |
| D-231 | `.github/terraform.pin` 이 없다 | 🔴 👤 | 샌드박스가 releases.hashicorp.com 에 접근하지 못해(프록시 403) 핀을 만들지 않았다. 없으면 CI `terraform` 잡이 빨간불(fail-closed, 의도) | `python scripts/pin_terraform.py` 후 커밋 (런북 0절) |
| D-232 | 마이그레이션 001~014 를 빈 DB 에 `migrate.py` 로 적용하는 첫 경로 + 일회성 컨테이너 실행 | ❓ 👤 Azure | D-202 의 실제 확인. api 이미지에 `psycopg` 가 있고 소스를 `:ro` 마운트해 실행하는 방식 [샌드박스에서 컨테이너 실행 불가 — 미확인] | 런북 5절 |
| D-233 | 스냅샷 복원 (`--data-only --disable-triggers`, TRUNCATE CASCADE 후 단일 트랜잭션) | ❓ 👤 Azure | 크기·시간·FK 순서·`ops` 스키마 표 전체 포함이 맞는지 미확인. 크면 기간 제한 필요 | 런북 4~5절 |
| D-234 | 스냅샷이라 `stale=true` 가 곧 나온다 | ⏸ | 증거 문서에 "스냅샷, 촬영 시각" 표기. 숨기려면 VM `.env` 의 `API_STALE_AFTER_SEC` 상향 | 런북 6절 |
| D-235 | VM 의 Compose·Docker 버전 | ❓ | Ubuntu 24.04 의 `docker-compose-v2` 패키지. `--wait`·병합이 Mac(v2.10.2)과 다르게 동작하는지(D-209). 샌드박스는 Compose v5.7.0 으로 병합 config 만 검증 | 런북 5절 |
| D-236 | SSH 터널로 Streamlit(웹소켓)이 열리는지 | ❓ 👤 | 일반적으로 되지만 이 구성에서는 미확인 | 런북 6절 |
| D-237 | 덤프 시 Mac 스택·마트 최신성 | ❓ 👤 | 사용자 확인 대기: Gold 마트 최신 시각, `gold_intraday` 다음 20분 실행 성공 여부(총정리 D) | 런북 4절 |
| D-238 | 예산 알림(50·80%) 생성 | 👤 | 런북 1절. 금액 ₩50,000 은 임의 값 | 포털 |
| D-239 | Step 12 비용 실측 | ⏳ 👤 | Korea Central `B2s_v2` 단가 미확인. 가동 시각·destroy 시각·비용 그래프를 `docs/evidence/step12/08_summary.md` 에 | 런북 7절 |
| D-240 | `deploy.yml`/OIDC 없음, GHCR 미사용 | ⏸ | 의도된 범위(ADR-0014 D12-6·D12-10). D-226(앱 등록 권한)은 필요해질 때 확인 | — |
| D-241 | 변이 시험 1건(e2e) 건너뜀 | ⏸ | `mutation_ci` 65건 중 64 검출, 1건은 `GOLD_TEST_DSN` 이 없어 건너뜀(기존 동작). CI 의 `test-pg` 에서 확인됨 | — |
| D-242 | `terraform apply` 가 MFA 오류로 거부됨(11-12) | ✅ 우회 경로 마련 / ⏳ 실행 대기 | `az login` 토큰의 `amr` 가 `['pwd']` 뿐이라 401 `RequestDisallowedByAzure`(MFA 필요). 같은 계정이 포털에서는 리소스 그룹을 바로 만든다 → 포털 세션 토큰에만 MFA 가 있다. 일반 창·시크릿 창·device code·`--scope` 로그인 모두 `mfa` 가 안 붙음. 패스키는 등록됨. 우회: Cloud Shell 에서 `apply`, Mac 은 `CLOUD_HOST`·`CLOUD_USER`·`CLOUD_RG` 환경 변수로 `cloud.sh` 사용(런북 3-B). 근본 원인(학교 테넌트의 MFA 적용 방식)은 확인하지 못했다 | 런북 3-B. 지울 때는 포털에서 리소스 그룹 삭제 |

## P. Step 12 실행 결과 + 11-12~11-17 운영 교훈 (11-17)

Step 12(Azure 서빙 계층 최소 증거 배포)는 **끝났다**: 증거 `docs/evidence/step12/`, 절차 `docs/runbooks/step12-azure.md`. 아래 항목은 그 과정과 같은 기간 Mac 운영에서 나온 것이다. 시각은 KST.

| ID | 항목 | 상태 | 현재 알려진 것 | 확인 방법 / 어디에 |
|---|---|---|---|---|
| D-250 | **Step 12 결과** | ✅ 2026-11-17 | VM 1대(Korea Central `B2s_v2`) + 네트워크·NSG 8개 리소스를 Terraform 으로 만들어 11-12 15:57 부터 가동, 컨테이너 `postgres`·`api`·`dashboard` healthy(증거 06), SSH 터널로 `/health`·`/ready` 200(08·09)·대시보드 7개 화면(11~17). 비용(RG 필터) 5일 ₩17,587.77(VM ₩15,724.70·Storage ₩1,100.32·VNet ₩762.73), 일 약 ₩3.5K(10). 11-17 `terraform plan` `No changes`·`apply` 8 added·`destroy` 8 destroyed·`az group exists` false(01~03·18·19). 대시보드는 **스냅샷(11-12 15:27)** 값이라 "오래됨/중단" 표시가 정상(증거 README). 올리지 않은 것: Kafka·Spark·Airflow·monitor·수집기. | 증거 README |
| D-251 | **Terraform state 가 Mac 에 없었다 / 어디에도 없다** | ✅ 우회 / ⏸ 재발 방지 선택 | Mac `terraform.tfstate`(592바이트, `serial 4`, `resources: []`, 11-12 15:30) 이후 VM 은 15:57 에 생겼다. Mac 에서 `plan` 은 `8 to add`(실제로는 이미 존재). Cloud Shell 홈도 비어 있었다. 첫 apply 의 경로(Cloud Shell 로 추정, `cloud.sh` 주석의 "3-B")와 state 위치는 **확인하지 못했다 [미확인]**. 처리: `apply` 하지 않고 리소스 그룹을 `az group delete` 로 지운 뒤 Cloud Shell 에서 처음부터 apply → plan → destroy 를 다시 해 증거를 만들었다. **교훈: `plan` 이 "전부 추가"라고 하면 먼저 `az group exists` 로 현실을 본다. apply 하면 충돌·중복 위험** | 선택: apply 직후 `terraform.tfstate` 를 안전한 곳에 복사 / 원격 backend(Storage, 비용 거의 0). 런북 7절 |
| D-252 | Mac 의 `az` 가 MFA 요구(`AADSTS50076`) | ✅ 우회 | 학교 계정 조건부 액세스로 Mac `az group delete` 가 막힘. **Cloud Shell(포털에서 이미 로그인)** 에서 `az`·`terraform` 을 실행하면 된다. 올릴 파일: `main.tf`·`variables.tf`·`outputs.tf`·`versions.tf`·`cloud-init.yaml.tftpl`·`terraform.tfvars`·SSH **공개키**(개인키·tfstate 는 올리지 않음). Cloud Shell 홈은 비어 있었다 | 런북 3절 |
| D-253 | **부활 검증**(apply → `cloud.sh up` → `cloud.sh tunnel`) | ✅ 2026-11-17 (사용자 보고, 캡처 없음) | 코드 한 벌로 만들고 올리고 접속했다. 막힌 곳: `cloud.sh up` 이 `ssh ... Operation timed out`(NSG 가 현재 IP 를 막음 — 허용 IP 는 `tfvars` 의 하나뿐). 안내대로 `allowed_ssh_cidr` 를 고쳐 apply 한 뒤 통과(원인 확정 기록 없음 [추정]). cloud-init 은 Docker 만 설치(앱·스냅샷은 `cloud.sh up`). **소요 시간 미측정 [미측정]**, `cloud.sh` 의 `ship` 이름은 주석에만 남아 있고 실제 서브커맨드는 `up`(주석 불일치, 사소) | 런북 4·6절 |
| D-254 | **비용 최종 확인** | ✅ 2026-11-18 사용자 확인: 비용 0(destroy 이후 청구 없음) | destroy(11-17 ~16:45) 뒤 일별 비용이 0 으로 떨어졌는지, 11-17 의 두 번째 VM(수 시간) 비용이 얼마인지 보지 않았다 [미확인]. 크레딧 잔액도 재확인 안 함(11-11 ₩101,300 에서 Step 12 약 ₩17.6K+α·Olist 월 ₩17.4K 가 빠진다 [추정]) | 11-19경 포털 비용 분석(범위: 내 계정, RG 필터) + Education Hub 잔액. D-227(Olist)과 같이 판단 |
| D-255 | **Bronze `OffsetOutOfRange` + `_spark_metadata` 함정** | ✅ 복구 / ⏸ 자동화 | 11-12 17:05 Mac 을 끈 뒤 11-16 10:08 에 재기동 → `spark-bronze` unhealthy: Kafka 보존 72시간이 지나 체크포인트 offset 의 데이터가 사라짐(`failOnDataLoss=true`, `streaming/bronze.py` 117줄 — 유실을 숨기지 않는 설계). 체크포인트만 옮기면 **새 데이터가 조용히 버려진다**: Spark FileStreamSink 는 `_spark_metadata` 의 마지막 커밋 batch id(2034) 이하의 새 batch(72…)를 "이미 처리됨"으로 건너뛴다. **체크포인트와 `data/bronze/<표>/_spark_metadata` 를 같이 옮겨야 한다.** 복구한 표: bus_arrival·seoul_incident·seoul_traffic(`data/_old/` 로 이동), 새 `event_date=2026-11-16` 폴더·Discord "해소" 로 확인. **내(Claude)가 처음에 체크포인트만 안내해 새 batch 가 건너뛰어진 것을 알아차렸다(sink 기록 2034 대 체크포인트 72) — 정정 완료.** 영구 결측: 11-12 17:05 ~ 재기동(11-16 ~10:30) 구간 중 Kafka 에 없던 것(수집기도 꺼져 있었다) | `docs/runbooks/step4-bronze.md` §8, `mac-on-off.md` §5. 자동화 후보: `BRONZE_FAIL_ON_DATA_LOSS` 환경 변수화(보류) |
| D-256 | **Kafka 보존 72시간 < 정지 시간** | ⏸ 선택 제안(미결정) | `config/topics.yaml` 의 `retention_ms: 259200000`(72시간); DLQ·audit 은 7일. 72시간을 넘겨 끄면 Bronze 가 못 따라가 복구 절차(D-255)가 필요하다. 상향(예: 7일)은 디스크를 더 쓴다. 토픽 단위로 실제 적용됐는지는 직접 확인하지 않았다 [미확인] | `kafka-configs --describe` 로 토픽 설정 확인 후 사용자 결정. 대안: 길게 끌 때만 절차를 따른다 |
| D-257 | **11-17 깨어난 뒤 Airflow scheduler 메모리 3.99 GiB, `docker compose stop` 실패** | ✅ 처리 / ⏸ 구조 해법 | 모니터를 약 하루 덮은 채 켜서(11-17 14:08) scheduler 가 한도 직전(3.99 GiB/4g). `docker compose stop` 이 daemon "did not receive an exit event" 로 실패 → `docker kill`/기다림 후 `docker ps -a` 로 `Exited (137)` 확인, Docker Desktop 재시작 없이 복구. 근본은 Mac 덮개 닫힘(D-222) + 깨어난 직후 Airflow 가 밀린 실행을 몰아서 돎(D-212 연쇄). **재기동 이후 안정성·메모리는 사용자가 보고하지 않았다 [미확인]** | `scripts/stack.sh stats`. D-212 의 해법(Gold Spark 를 scheduler 밖 일회성 컨테이너로) 은 계속 보류 |
| D-258 | `silver_verify` 가 `1800 s` 타임아웃(긴 정지 뒤 `silver_reconcile_daily`) | ❓ 재발 여부 미확인(사용자가 11-19 에 Airflow UI 확인 예정) | 11-12 정지 뒤 04:30 `silver_reconcile_daily` attempt=2 로그에서 `silver_verify` 1800초 제한 초과. 긴 정지 뒤 데이터가 많아 오래 걸리는 것으로 보인다 [추정]. 이후 실행 성공 여부 미확인 | Airflow UI 의 `silver_reconcile_daily` 최근 실행. 필요하면 제한 상향 |
| D-259 | **Gold 신선도·`gold_daily` 성공 여부** | ✅ 2026-11-18 | `gold_daily`(01:00 KST) 마지막 성공이 11-11 13:39 로 DQ 알림에 나왔다. 11-12 이후 정지·재기동 후 `ops.load_run`(`job=gold_load`) 이 최근 구간을 덮는지, 다운스트림 `require_recent_load`(36시간 창) 를 통과하는지 보지 않았다 [미확인]. 대시보드 건강 화면의 마트 시각으로 확인 | `ops.load_run`(status=ok) 확인: `gold_daily` 마지막 성공 11-18 01:02 KST, `gold_intraday` 11-18 14:28 KST. 11-12 이후 재기동 뒤에도 적재가 이어짐 |
| D-260 | **덮개를 닫고 돌리기(`pmset disablesleep`)** | ✅ 문서(11-17) / ⏸ 실측 | `sudo pmset -a disablesleep 1` 후 `0` 으로 되돌림. 전원 연결·통풍 필수(닫힌 Mac 은 열 배출이 나쁘다 [추정]). 외부 모니터 연결도 대안. 화면만 꺼지는 것(덮개 열림)은 `caffeinate -i` 로 충분. **덮개 닫고 하루 이상 돌린 실측은 없다 — 11-16~17 에 덮개를 하루 가까이 덮어 둔 것은 절전 상태였던 것으로 보인다 [추정]** | `docs/runbooks/mac-on-off.md` §4 |
| D-261 | **수집 공백 기록(포트폴리오 숫자 제외 구간)** | ⏸ | 11-12 ~17:05 ~ 11-16 ~10:30 은 수집·처리가 거의 없다(약 4일 17시간). 11-07(D-222 절전)·11-09~10(OOM)도 결측이 있다. 하루 수집 레코드 수·성공률·DQ 통과율을 7-13 에 쓸 때 **이 구간을 평균에서 빼고 명시**한다 | 7-13 숫자 모음. 정확한 시각은 `data/bronze` 폴더의 `event_date` 와 Discord 알림으로 |
| D-262 | **증거 이미지 원본 관리** | ✅ 2026-11-17 | 저장소에는 **마스킹한 이미지만** 넣었다(계정 이름 3곳·허용 SSH IP·VM 공인 IP). 원본(채팅 업로드)은 저장소에 두지 않는다. 마스킹은 불투명 박스 — 블러는 복원될 수 있어 쓰지 않았다. 처음 만든 apply 화면은 박스 위치가 틀려 IP 가 남아 있어 **다시 가리고 눈으로 확인**했다. 향후 캡처: `terraform plan` 의 `source_address_prefix`·`apply` 의 `public_ip` 줄을 가린다 | `docs/evidence/step12/` |
| D-263 | 11-09~11-11 에 올린 이전 스크린샷(7장) | ❓ 정리 안 함 | 세션 업로드 폴더에 Step 12 이전 시기 이미지 7장이 더 있다(예: 11-09~11-11 업로드). Step 12 증거가 아니라고 보고 저장소에 넣지 않았고 내용도 이번에 열어 보지 않았다 | 사용자가 포트폴리오에 쓸 것이 있으면 알려 주면 마스킹해 `docs/evidence/` 에 추가 |
| D-264 | 🔴 **Step 12 코드가 샌드박스·zip 에 없다** | ✅ 2026-11-17 해소 (전체 저장소 zip 으로 점검·정리) | 이 저장소 사본(샌드박스)에는 `infra/`(Terraform), `scripts/cloud.sh`, `scripts/cloud_remote.sh`, `docker-compose.cloud.yml`, ADR-0014, Step 12 시험이 **없다**. 사용자 Mac 에는 있다(`infra/terraform/{main,variables,outputs,versions}.tf`·`cloud-init.yaml.tftpl`·`terraform.tfvars.example`, `scripts/cloud.sh`·`cloud_remote.sh` 확인됨; `docker-compose.cloud.yml`·시험·변이는 `cloud.sh` 가 참조하므로 있을 것으로 보이나 내용은 못 봄 [미확인]). 그래서 11-17 마무리 때는 **전체 zip 을 만들지 않고 문서만 담은 패치 zip** 을 보냈다 — 전체 zip 을 풀면 샌드박스의 오래된 `.gitignore`·`layers_plugin.py`·`mutation_ci.py` 가 Mac 의 Step 12 판을 덮어쓸 위험이 있다(`.gitignore` 에서 tfstate·tfvars 제외가 사라지면 IP 포함 파일이 커밋될 수 있다). Step 12 코드의 시험·변이 시험·`ruff`·`ci_checks` 통과는 **이 샌드박스에서 검증하지 못했다** | 사용자가 위 파일 목록을 첨부(tfstate·tfvars·`.terraform` 제외)하면 병합 후 시험·변이 검증과 진짜 전체 zip 을 만든다 |
| D-265 | Step 12 비밀 파일이 git 에서 제외되는지 | 👤 확인 | 제외돼야 하는 것: `infra/terraform/terraform.tfvars`(허용 IP 포함), `terraform.tfstate*`, `.terraform/`. 커밋해야 하는 것: `.terraform.lock.hcl`(Terraform 이 권장). 확인: `git check-ignore -v infra/terraform/terraform.tfvars infra/terraform/terraform.tfstate` 가 각각 규칙을 보여 줘야 하고 `git status -sb` 에 이 파일들이 안 보여야 한다. 증거·문서에는 IP 를 넣지 않았다 | 사용자 Mac |
| D-266 | 장기 연속 수집 계획(4주 이상) | ⏳ 👤 | 11-18 사용자: 하루 이상 켜 둔 적이 없어(이동 등) 곧 오래 돌릴 예정. 이상 탐지 기준선이 28일 미만이면 `candidate` 라 4주 이상 이어야 한다(ADR-0010 D8-11). 시작 전 확인: Kafka 보존 72시간(D-256, 미결정), Mac 절전 방지(D-260, `mac-on-off.md`), Airflow 메모리(D-211) | 수집 후 README 규모 표 갱신(조회문은 공개 가이드) |
| D-267 | 온전히 채워진 수집일 | ✅ 2026-11-18 사용자 확인 | 11-03~11-18 중 하루가 온전히 채워진 날은 11-04·05·06 뿐. README 의 규모 수치는 최댓값·가동일수로 적고 "하루 평균"은 쓰지 않았다. 장기 수집 뒤 다시 계산(D-266) | — |
