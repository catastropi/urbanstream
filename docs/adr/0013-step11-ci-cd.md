# ADR-0013 — Step 11: 시험 계층 정리와 GitHub Actions CI/CD

날짜: 2026-11-08. 상태: 구현됨(샌드박스 검증 · **GitHub Actions 실제 실행은 아직 한 번도 안 함** — 사용자가 push 해서 초록불을 봐야 확인됨, `docs/DEFERRED.md` K장).
사용자 승인: 2026-11-08 ("전부 승인 — 단, 이전 스텝들과 호환성 문제가 생기면 안 된다"). 이 문장이 설계의 제약이다: **기존 시험 710개의 이름·결과·실행 방법을 바꾸지 않는다.**
사실 표기: **[샌드박스]** 이 환경에서 실행해 확인 / **[GitHub 미확인]** Actions 에서만 확인 가능 / **[추정]** 근거 없이 합리적으로 짐작.

## 맥락
Step 2~10 의 시험은 사용자 Mac 과 샌드박스에서 손으로 돌렸다. 저장소는 private(`catastropi/urbanstream`), GitHub Free 플랜이다. 배포할 서버는 없고(스택은 Mac 의 compose) 이미지를 올릴 곳은 GHCR 이다.
[GitHub 문서 기준, 2026-11 검색] private 저장소 Free 플랜은 Linux 2,000분/월, public 은 무제한. GHCR 저장·전송은 "현재 무료"이며 변경 시 1개월 전 공지. Node 20 액션은 2026-09-16 부터 러너에서 제거 → checkout v6·setup-python v6·setup-java v5 사용.

## 결정

| ID | 결정 | 이유 | 한계 / 되돌리기 |
|---|---|---|---|
| D11-1 | 워크플로 3개: `ci.yml`(PR·main push·수동), `weekly.yml`(주 1회 일 03:17 KST·수동), `release.yml`(태그 `v*`) | PR 마다 도는 것은 빠르게, 무겁고 드문 것(Spark, 변이 API 전체, 이력 전체 시크릿 스캔, Python 3.11/3.13)은 주간으로 | 되돌리기: `.github/workflows/` 파일 삭제. 앱 코드에 영향 없음 |
| D11-2 | **CI 에 실제 API 키·Secrets 를 쓰지 않는다.** 참조하는 비밀은 `GITHUB_TOKEN`(release 의 GHCR 로그인) 뿐 — 시험이 `secrets.*` 를 이것으로 제한 | 시험은 가짜 응답·녹화본·서비스 컨테이너로 충분. 외부 API 호출은 한도·키 유출 위험만 늘림. PR 에서 비밀을 쓰면 포크 PR 위험 | `api-contract-watch`(실제 API 주기 호출)는 **하지 않음** → DEFERRED D-187 |
| D11-3 | `ci-ok` 집계 job: `if: always()`, 모든 job 을 `needs`, 하나라도 failure/cancelled 면 실패. **브랜치 보호의 필수 체크는 이 하나만 지정** | 경로에 따라 건너뛴 job 이 '성공'으로 보이거나 이름이 바뀌어도 보호가 깨지지 않게 | 브랜치 보호는 사용자가 GitHub 설정에서 선택(Free 플랜 private 은 기능 제한 가능 [미확인]) |
| D11-4 | 린트는 **치명 규칙만**(ruff E9·F63·F7·F82, `ruff==0.16.9`), 포매터 없음 | F401/F811 은 `from pg_support import pg` fixture 패턴에서 오탐 129건. 전 파일 재포맷은 diff 가 커 되돌리기 어려움 [샌드박스: 현재 트리 `All checks passed`] | 규칙 확장은 `ruff.toml` 한 줄. 시험(`test_ci_checks`)이 규칙 집합을 고정하므로 바꿀 때 시험도 같이 바꾼다 |
| D11-5 | 계층 마커(`unit contract integration spark e2e recovery api dashboard data`)는 **플러그인**(`tests/support/layers_plugin.py`, `pytest.ini` 의 `-p layers_plugin`)이 디렉터리 이름으로 붙인다. 시험 파일은 한 줄도 안 고침 | 710개 시험의 ID·결과를 보존(호환 제약). `__init__.py`·conftest 를 늘리면 `tests/contract` 의 `from conftest import` 가 깨짐(HANDOFF 함정) | **[샌드박스]** 수집 ID 710개가 원본과 동일, 추가분만 106개. **[Mac·Spark 컨테이너 미확인]** `spark_test.sh` 가 새 pytest.ini 로 도는지. 되돌리기: `pytest.ini` 에서 `-p layers_plugin` 제거 — 마커 선택(`-m`)만 못 쓰게 됨 |
| D11-6 | `data` 계층 = 기존 시험의 **재분류**(6개 파일), `e2e` = **새 시험 1파일 4개**: 실제 uvicorn 서버 + 실제 Postgres + 대시보드 클라이언트 + Tableau 내보내기 | HANDOFF D-173 이 비워 둔 곳. Kafka→Spark→Gold e2e 는 CI 에서 비용이 커 제외(D-186) | e2e 는 `GOLD_TEST_DSN` 없으면 skip (기존 pg 시험과 같은 방식) |
| D11-7 | Postgres 는 **서비스 컨테이너**(`imresamu/postgis:17-3.6.1-bookworm` = compose 와 같은 이미지, 시험이 둘의 일치를 대조), `ALLOW_ROLE_TESTS=1` | Mac 은 PG17/PostGIS 3.6.1, 샌드박스는 PG16/3.4 → CI 가 처음으로 운영과 같은 버전에서 시험을 돈다(운영과 같은 버전에서의 시험 확인 가능) | 역할 시험은 클러스터 전역 role 을 지우므로 **일회용 CI 컨테이너에서만** 켠다. 로컬 기본값은 꺼짐 그대로 |
| D11-8 | CD = 태그 `v<major>.<minor>.<patch>` 푸시 → **api·dashboard 이미지만** GHCR(`ghcr.io/<owner>/urbanstream-<name>:<ver>`, `:sha-<7>`), linux/amd64. 푸시 전에 이미지 검사(`.env`·`data`·`tests`·`sql`… 없음, 비루트 10001). `latest` 태그 없음. `packages: write` 는 이 job 에만, PAT 없음 | 배포 서버가 없으므로 '게시'까지만. Airflow·Spark 이미지는 크고 Mac 에서 직접 빌드(D-186) | **[GitHub 미확인]** 실제 빌드·푸시. 되돌리기: 태그·패키지 버전 삭제(GitHub UI), `release.yml` 삭제. Mac(arm64) 은 이 이미지를 쓰지 않음 — compose 는 로컬 빌드 그대로 |
| D11-9 | 액션은 1st-party(`actions/*`)만, **현재 `@vN` 태그**. 40자 SHA 고정은 `scripts/pin_actions.py`(사용자 Mac 에서 실행)로, 갱신은 Dependabot(주 1회, 최대 5건) | 샌드박스는 github.com 의 태그→SHA 를 조회할 수 없어 **SHA 를 지어내지 않았다** | ⚠ 승인된 결정 9("SHA 고정")와 **다름** — 사용자가 `pin_actions.py` 를 돌려야 완성(D-183). 시험은 SHA 또는 `vN` 형식만 허용하고 `main`/`latest` 는 거부 |
| D11-10 | 시크릿 스캔 = **gitleaks 고정 바이너리**(`scripts/ci_gitleaks.py`가 `.github/gitleaks.pin` 의 버전·sha256 으로 내려받아 검증) + 자체 `tracked` 검사(`.env`·`data/` 실파일·캐시가 git 에 올라갔는지). PR 은 `base..sha` 범위, 주간은 전체 이력. 허용: `tests/fixtures/recorded/` 와 문자열 `ci-not-a-secret` 만 | `gitleaks-action` 은 조직 계정이면 라이선스가 필요. 고정 해시면 공급망 위험 최소. **핀이 없으면 실패(fail-closed)** — 검사를 못 하면서 초록불이 나오는 것이 가장 나쁨 | 🔴 `gitleaks.pin` 은 사용자가 `scripts/pin_gitleaks.py` 로 만들어 커밋해야 함(샌드박스에서 릴리스를 조회하지 못함, D-184). 그 전에는 `secrets` job 이 빨간불 — **의도된 동작** |
| D11-11 | 경로 필터(`scripts/ci_changes.py`): api/dags/docker 그룹은 해당 파일이 바뀔 때만 무거운 job 실행. **모르면 전부 실행(fail-open)**, `.github/`·CI 스크립트·`pytest.ini`·`tests/support/` 변경은 항상 전부 | 분(minute) 절약. 잘못 건너뛰어 빨간불을 놓치는 것보다 한 번 더 도는 편이 낫다 | `docs/**`·`*.md` 만 바뀐 push/PR 은 워크플로 자체를 건너뜀(`paths-ignore`). 되돌리기: `changes` job 출력 대신 `true` 고정 |
| D11-12 | 시험 추가: `test_workflows.py`(권한 읽기전용·timeout·액션 고정·비밀 제한·위험 트리거 없음·release 가 태그에서만/쓰기 권한은 image job 만·집계 job), `test_ci_checks.py`, `test_ci_tools.py`, `test_layers_plugin.py` + e2e. **변이 시험 `scripts/mutation_ci.py` 35건** | 실행할 수 없는 워크플로는 '위험한 모양이 없는가'를 정적으로 지킨다 | 아래 비용·검증 |

## 비용과 시간
- PR 한 번(코드 변경, 모든 job): 병렬 약 6~10분 벽시계, 소모 **약 40~60분 [추정]** → private Free 2,000분/월이면 월 30~50회 수준. 문서만 바뀌면 0분. 주간 점검 약 30~40분 × 월 4~5회 [추정]. **실제 값은 첫 실행 후 Actions 의 사용량 화면으로 확인(D-188)**.
- 분이 모자라면: ① `test-minimal` 을 주간으로 옮김 ② `mutation-api` 를 PR 에서 빼고 주간만 ③ 저장소를 public 으로(분 무제한이지만 코드·이력이 공개 — 키 스캔이 먼저).
- GHCR: 현재 무료(문서 기준), 용량이 쌓이면 오래된 태그를 UI 에서 삭제.

## 검증 결과 [샌드박스, Python 3.13 venv + PG16/PostGIS, 2026-11-08]
- 전체 시험 **814 passed / 3 skipped / 0 failed** (PG+역할 시험 켬). 수집 ID: 원본 710개 전부 그대로, 추가 106개(e2e 4, 워크플로 27, ci_checks 27, ci_tools 41, layers 7).
- 변이: `mutation_ci.py` 35/35, `mutation_dashboard.py` 33/33, `export_openapi.py --check` 통과, `docker compose config`(더미 환경 파일, 전체 프로파일) 통과, `ruff` 통과.
- 워크플로 YAML 은 파싱·정적 시험만 했다. **GitHub 러너에서 실행해 보지 못했다.**
