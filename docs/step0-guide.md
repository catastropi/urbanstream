# Step 0 — API 스모크 테스트 가이드 (2026-11-02 갱신)

Mac(zsh) + PyCharm + Conda 기준. 2026-10-13 판은 "버스는 TAGO 가 메인" 이라고 적고 있었으나 실제 테스트로 틀린 것이 확인돼서 교체했다.
Step 0 의 결과는 이 문서가 아니라 [api-catalog](api-catalog/README.md) 가 기준이다.

## 확정된 것

- 서울 버스는 `ws.bus.go.kr`(data.go.kr 에서 승인받은 '서울특별시_버스위치/도착정보조회 서비스', `DATA_GO_KR_KEY`)를 쓴다.
  **TAGO 는 서울을 다루지 않는다** (도시코드 138건에 서울 없음). TAGO 코드는 서울 밖으로 넓힐 때(Phase 2)를 위해 보관만 한다.
- 지하철(TAGO 지하철정보)은 Phase 2 로 보류.
- 서울 도로소통/돌발은 **XML 로만** 호출된다 (`json` → `ERROR-301`).
- 도로소통은 응답에 시각이 없다 → event_time 은 수집 시각.
- 노선 753번 = `busRouteId 100100118`. 위치의 `nextStId` ↔ 도착의 `stId`, 위치의 `vehId` ↔ 도착의 `vehId{n}` 로 조인된다.

## 실행

```zsh
conda activate urbanstream          # Python 3.11. base 로 실행하지 않는다 (Spark 단계부터 문제가 된다)
pip install -r requirements-dev.txt
python scripts/smoke_test.py check-env          # .env 점검 (네트워크 호출 없음)
python scripts/smoke_test.py run                # 5개 API 를 1회씩 호출, data/smoke/report_*.md 생성
python scripts/smoke_test.py run --only bus_position,bus_arrival
```

PyCharm 의 실행(▶) 버튼은 서브커맨드 없이 실행돼 argparse 오류가 난다. 터미널에서 위처럼 실행한다.
zsh 에서 URL 의 `?` `&` 가 해석되어 `no matches found` 가 나면 URL 을 따옴표로 감싸거나 `curl -G --data-urlencode` 를 쓴다.

## 아직 남은 측정

[runbooks/step0-closeout.md](runbooks/step0-closeout.md) 를 따른다: `cadence`(갱신 주기), `kma-lag`(기상청 제공 시각), 호출 한도, 링크 지오메트리, 노선·정류소 서비스 승인.

## 자주 만난 오류

| 증상 | 원인 | 해결 |
|---|---|---|
| `INFO-100 인증키가 유효하지 않습니다` | `SEOUL_API_KEY` 가 열린데이터광장 키(30자)가 아님 | 열린데이터광장에서 발급한 키로 교체 |
| `ERROR-301 파일타입 값이 누락...` | 도로소통/돌발은 XML 만 지원 | URL 의 `json` → `xml` (현재 스크립트는 이미 xml) |
| `code=12 NO_OPENAPI_SERVICE_ERROR` | 존재하지 않는 경로 | `ws.bus.go.kr/api/rest/...` 사용 |
| 버스 `401 등록되지 않은 서비스키` | 노선/정류소 조회는 신청하지 않은 별도 서비스 | 위치/도착은 그대로 사용, 노선/정류소 서비스는 추가 신청 후 승인 대기 |
| 키 재발급 직후 401 | 반영 지연 | 몇 분 뒤 재시도 |

## 키 취급

- `.env` 는 git 에 올리지 않는다 (`.gitignore`). 압축해서 공유할 때도 `.env` 를 빼고 `.env.example` 만 넣는다.
- 채팅·이슈·리포트에 키 값을 붙여넣지 않는다. `smoke_test.py` 출력과 리포트의 키는 `***KEY***` 로 마스킹되며, 이 마스킹(`redact`)을 지우지 않는다.
- 채팅에 노출된 적이 있는 키는 재발급한다.
