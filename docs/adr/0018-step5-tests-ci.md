# ADR-0018: STEP 5 테스트 숫자 자동화, 데이터 로직 뮤테이션, 린트 확대

- 상태: 채택 (2026-11-28). 설계안의 결정 1~6 을 권고안대로 진행했다.
- 관련 코드: `scripts/test_inventory.py`, `scripts/mutation_data.py`, `scripts/doc_traces.py`, `tests/fixtures/doc_traces_baseline.json`,
  `ruff.toml`, `scripts/ci_changes.py`(`data` 그룹), `.github/workflows/ci.yml`(hygiene 두 단계, `mutation-data` job, junit 요약),
  `.github/workflows/weekly.yml`(Spark 뮤테이션, `mutation_ci.py`). 설명: `docs/testing.md`. 기록: DEFERRED V장(D-304~D-311).

## 1. 맥락

STEP 0 감사(H-3)에서 문서마다 테스트 숫자가 달랐다. README 는 "함수 702개, 551 passed / 89 skipped" 였고, 실제로는 그때 703개였다.
언제 어디서 돌렸는지 적혀 있지 않아 어느 숫자가 맞는지 판단할 수 없었다. "변이 시험 65건 전부 검출" 은 `mutation_ci.py` 의 숫자로,
대상이 워크플로·gitleaks·compose 같은 CI 안전장치라 데이터 처리 로직과는 관계가 없었다. 그 스크립트는 CI 에서 돌지도 않았다.

STEP 1(재처리)과 3B(작업 잠금)로 데이터 정확성을 지키는 코드가 늘었는데, 그 코드를 망가뜨렸을 때 테스트가 잡는지는 확인한 적이 없었다.
ruff 는 치명 오류 네 종류만 봤고, 안 쓰는 import 가 44곳 쌓여 있었다.

제약: 11-25 부터 STEP 2 수집이 돌고 12-09 부터 무거운 측정을 한다. 이 단계의 변경이 돌아가는 스택을 바꾸면 안 된다.

## 2. 결정

| # | 결정 | 이유 |
|---|---|---|
| 1 | 문서와 자동 대조하는 숫자는 `def test_` 개수(AST)뿐이다. 실행 결과는 날짜·환경과 함께 `docs/testing.md` 기록 표에 적는다 | 함수 수는 환경과 무관해 CI 에서 대조할 수 있다. passed/skipped 는 PG·Spark·Kafka 유무로 달라져 대조하면 늘 깨진다 |
| 2 | README 와 `docs/testing.md` 의 숫자 칸은 표시(`<!-- tests:…:start -->`) 사이만 스크립트가 쓴다 | 손으로 고칠 일이 없다. 칸 밖의 글은 사람이 쓴다 |
| 3 | 데이터 로직 뮤테이션은 새 스크립트(`mutation_data.py`)로 두고, 데이터 코드가 바뀐 push 에서만 CI 가 돌린다. Spark 항목은 weekly | API 뮤테이션과 같은 방식이다. Spark 3건은 한 건에 1분 넘게 걸려 매 push 에는 무겁다 |
| 4 | ruff 에 F401·F841·F541 를 더하고, 운영 코드까지 한 번에 고친다. F811 은 켜지 않는다 | 고친 곳은 import 줄과 쓰지 않는 대입 하나뿐이라 동작이 바뀌지 않는다. F811 119건은 모두 `from pg_support import pg` fixture 패턴이다 |
| 5 | 작업 환경 흔적(감사 6-1)은 지금은 '늘지 않게' 만 막는다(파일별 기준 개수). 0건으로 만드는 것은 STEP 6 | 고치는 일은 파일별로 판단이 필요하다(감사 6-4). 그 전에 새 문서가 흔적을 더하는 것은 막는다 |
| 6 | `mutation_ci.py` 를 weekly 에 넣는다 | 65건이 있는데 돌리는 곳이 없었다. 이번에 ci.yml 을 바꾸면서 정의 2건이 실제로 어긋났다 |

## 3. 데이터 로직 뮤테이션 27건

영역별: 재처리 10(포인터 7, 실행 이력 2, 재빌드 1), 적재 5, DQ 5, 증분 3, 중복 제거 2, 잠금 1. 목록은 `python scripts/mutation_data.py --list`.
첫 실행에서 25건을 잡고 2건을 놓쳤다. 놓친 것과 보강은 `docs/testing.md` 3장에 있다.

뮤테이션 사본에는 `data/` 를 넣지 않는다. Mac 에서 돌려도 실데이터를 복사하지 않는다.

## 4. 결과

- 클라우드(Python 3.12, PostgreSQL 16 + PostGIS 3, Spark 4.1.3 local)에서 전체 테스트, 뮤테이션 3종(data·ci·dashboard), ruff, 숫자 대조, 흔적 검사를 돌렸다. 숫자는 `docs/testing.md` 4장.
- CI 와 weekly 가 GitHub 에서 실제로 초록인지는 push 뒤 Actions 화면에서 확인해야 한다(이 작업 환경은 Actions API 를 읽지 못했다).

## 5. 하지 않은 것

| 항목 | 이유 |
|---|---|
| README 의 '시험' 표현 전체를 '테스트' 로 | STEP 6 문서 정리에서 한 번에. 이번에 새로 쓰거나 고친 줄만 '테스트' 로 썼다 |
| Mac 에서 전체 테스트 | 측정 기간에 CPU 를 쓰지 않는다. Mac 에서는 단위 테스트 일부만 확인한다(적용 절차) |
| 커버리지 비율 | 숫자 하나로 품질을 말하기 어렵고, 뮤테이션이 더 직접적인 근거다. 필요하면 STEP 8 에서 |
| 포매터 | ADR-0013 D11-4 그대로 |
