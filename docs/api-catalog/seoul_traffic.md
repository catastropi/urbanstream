# seoul_traffic — 서울 실시간 도로 소통 정보

계약: `traffic_speed_v1` · 파서: `src/urbanstream/contracts/traffic.py` · 샘플: `tests/fixtures/recorded/seoul_traffic/`

## 개요

| 항목 | 내용 | 근거 |
|---|---|---|
| 공식명 / 서비스명 | 서울시 실시간 도로 소통 정보 / `TrafficInfo` | 호출 성공 |
| 제공기관 | 서울특별시 (서울 열린데이터광장) | 호출 URL |
| 쓰이는 문제 | A 혼잡 탐지, B 돌발 영향 분석, D 기상-교통 결합 | Blueprint |
| 인증 | `SEOUL_API_KEY` (열린데이터광장 키, 30자) — data.go.kr 키와 별개 | 호출 성공 |
| 호출 | `http://openapi.seoul.go.kr:8088/{KEY}/xml/TrafficInfo/1/5/{link_id}` | 2026-11-01 18:07 PASS |
| 응답 형식 | **XML만 지원**. `json` 으로 부르면 `ERROR-301` | 2026-10-28 15:17 관측 |
| 호출 한도 | **미확인.** 이용약관 제6조에 숫자가 없다(서울시가 횟수를 지정할 수 있고 과다 호출은 인증키를 제한할 수 있다고만 함). 수집기는 보수적으로 일 1,000건을 상한으로 둠 | 2026-11-03 |
| 갱신 주기 | **약 150초(2.5분)로 추정** — 2026-11-02 13:04~13:23 `cadence` 1회: 변경 8회, 간격 120/179/121/179/120/180/120초(평균 146초). 60초 해상도라 120·180 교대로 보임. 퇴근 시간대 재측정 전까지 잠정 | 2026-11-02 |
| 호출 지연 | 120 ms (1회 관측) | 리포트 |

## 응답 구조 (관측)

링크 1개당 1행. 정상 응답:

```
<TrafficInfo><list_total_count>1</list_total_count>
  <RESULT><CODE>INFO-000</CODE><MESSAGE>정상 처리되었습니다</MESSAGE></RESULT>
  <row><link_id>1220003800</link_id><prcs_spd>16</prcs_spd><prcs_trv_time>675</prcs_trv_time></row></TrafficInfo>
```

## 시간·공간 정보

- **시간: 응답에 시각이 없다.** 그래서 event_time 은 수집 시각을 쓴다 (확정 결정, ADR-0001).
- 공간: `link_id` 뿐이다. 좌표/지오메트리는 이 API에 없다 → **링크 지오메트리 데이터셋이 따로 필요 (미확보)**.
  후보: 열린데이터광장 '소통 돌발 도로별 링크 정보'. 절차는 `docs/runbooks/step0-closeout.md`.

## 오류·장애 (관측한 것만)

| 상황 | 응답 | ApiStatus.kind |
|---|---|---|
| 키 오류 | 루트가 `<RESULT>` (XML 선언 없음), `INFO-100`, 메시지가 CDATA | `auth` |
| 인자 오류(TYPE) | `<response><RESULT>` 안에 `ERROR-301` | `param` |
| 정상 | `INFO-000` | `ok` |
| 데이터 없음 | `INFO-200` 으로 **알려져 있으나 미관측** | `empty` (관측 시 확정) |
| 그 밖의 코드 | — | `unknown` (code/message 보존) |

HTTP 오류와 깨진 XML 도 예외 없이 `ApiStatus`(`server_error`, `malformed` 등)로 표현한다.

## 계약 (traffic_speed_v1)

- event_time: 수집 시각(fetched_at)을 분 단위로 절삭 — 소스가 시각을 주지 않음
- dedup_key: link_id | YYYYMMDDHHMM
- source_system: `seoul_traffic`

| payload 필드 | 원본 필드 | 타입 | 설명 |
|---|---|---|---|
| `link_id` | `link_id` | string | 도로 링크 ID (필수. 없으면 행 reject) |
| `speed_kmh` | `prcs_spd` | double? | 구간 속도(km/h). **0 을 결측으로 바꾸지 않는다** — 0 이 정체인지 결측인지 미확인 |
| `travel_time_sec` | `prcs_trv_time` | long? | 구간 통과 소요시간(초) |

## 측정 예정 / 미확인

- [x] 갱신 주기 1차 측정 완료 (낮 13시, 위 표). [ ] 퇴근 시간대(17:30~19:30) 재측정으로 확정
- [ ] 호출 한도(일/월)
- [ ] 0 속도의 의미, 링크 구간 길이·지오메트리
- [ ] 한 호출에 여러 링크를 요청할 수 있는지 (`1/5/` 의 범위 인자와 `{link_id}` 조합의 정확한 의미)
