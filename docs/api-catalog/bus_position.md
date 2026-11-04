# bus_position — 서울 버스위치 (노선별)

계약: `bus_position_v1` · 파서: `src/urbanstream/contracts/bus_position.py` · 샘플: `tests/fixtures/recorded/bus_position/`

## 개요

| 항목 | 내용 | 근거 |
|---|---|---|
| 공식명 | 서울특별시_버스위치정보조회 서비스 (data.go.kr) | 승인 목록 |
| 서비스 / 오퍼레이션 | `ws.bus.go.kr` / `getBusPosByRtid` | 호출 성공 |
| 쓰이는 문제 | C 대중교통 지연 탐지, A 혼잡(버스 속도) | Blueprint |
| 인증 | `DATA_GO_KR_KEY` | 호출 성공 |
| 호출 | `http://ws.bus.go.kr/api/rest/buspos/getBusPosByRtid?ServiceKey=...&busRouteId=100100118` | 2026-11-01 21:53 PASS |
| 응답 형식 | XML (`ServiceResult`) | 관측 |
| 호출 한도 | **상세기능별 일일 트래픽 1,000건** (사용자가 공공데이터포털 화면에서 확인, 2026-11-03; `getBusPosByRouteStList`, `getBusPosByRtidList` 각각 1,000). 포털의 상세기능 이름(`...List`)과 수집기가 부르는 주소(`ws.bus.go.kr/.../getBusPosByRtid`)가 'List' 만큼 다르다 — 같은 기능으로 보이나 포털의 호출 통계와 `collector status` 숫자를 비교해 확인 필요 | 2026-11-03 |
| 갱신 주기 | **차량 보고 주기 약 20초로 추정** (2026-11-03 15초 간격 40회): `dataTm` 이 그대로인 쌍 19%(118/611) → 1−15/T 로 T≈18.5초, 바뀐 쌍의 증가량 중앙값 21초, 호출 시점의 데이터 나이 중앙값 19초(p95 32). 60초 간격 측정(11-02)에서는 나이 중앙값 10초. 같은 차량의 `dataTm` 이 호출 사이 **1초 거꾸로 간 쌍**이 있었다(최소 −1) → Silver 는 차량별 `dataTm` 이 단조 증가한다고 가정하면 안 됨. 수집은 90초(한도 가정 때문) → 보고의 약 1/4~1/5만 봄 | 2026-11-03 |
| 호출 지연 / 크기 | 66 ms / 8,367 B (13대) | 리포트 |
| 테스트 노선 | 753번 = `busRouteId 100100118` | `.env` |

## 응답 구조 (관측)

`<ServiceResult><comMsgHeader/><msgHeader><headerCd>0</headerCd>...<itemCount>0</itemCount></msgHeader><msgBody><itemList>…`

- **`itemCount` 는 항상 0 으로 온다** (13건 응답에서도). 건수는 `itemList` 를 센다.
- 응답에 **노선 ID 가 없다.** 호출 인자의 `busRouteId` 를 payload 의 `route_id` 로 받는다.
- 일요일 21:53 기준 753번 13대. 심야에는 차량 수가 줄어든다 (0대 응답은 미관측 → `empty` 로 처리하는 것은 가정).

## 시간·공간 정보

- 시간: `dataTm` (YYYYMMDDHHMMSS, KST) = 차량의 GPS 보고 시각.
- 공간: `gpsX/gpsY` = 경도/위도(WGS84), `posX/posY` = TM(EPSG:5181, 변환 오차 최대 6 m, 13점).
- 정류소 연결: `nextStId`/`lastStnId` 는 버스도착의 `stId` 와 같은 ID 체계 (12/12 일치).

## 오류·장애

| 상황 | 응답 | ApiStatus.kind |
|---|---|---|
| 정상 | `headerCd` = 0 | `ok` (0건이면 `empty`) |
| 등록되지 않은 서비스키 | HTTP 401, JSON `{"error":"Unauthorized","message":"...등록되지 않은 서비스키","status":401}` | `auth` |
| 잘못된 경로 | JSON `OpenAPI_ServiceResponse`, `returnReasonCode` = 12 (`NO_OPENAPI_SERVICE_ERROR`) | `no_service` |
| 그 밖의 `headerCd` | **미관측** | `unknown` (code/message 보존) |

## 계약 (bus_position_v1)

- event_time: dataTm (차량별 GPS 보고 시각, KST)
- dedup_key: vehId | dataTm
- source_system: `bus_position`
- 샘플 13건에서 `vehId|dataTm` 모두 유일.

| payload 필드 | 원본 필드 | 타입 | 설명 |
|---|---|---|---|
| `route_id` | (호출 인자 `busRouteId`) | string? | 응답에 없어 호출 맥락에서 받음 |
| `veh_id` | `vehId` | string | 차량 ID (필수) |
| `plain_no` | `plainNo` | string? | 번호판. 공백 제거 |
| `lon` | `gpsX` | double? | 경도 |
| `lat` | `gpsY` | double? | 위도 |
| `tm_x` | `posX` | double? | TM x |
| `tm_y` | `posY` | double? | TM y |
| `section_id` | `sectionId` | string? | 구간 ID |
| `sect_ord` | `sectOrd` | long? | 구간 순번 |
| `last_stop_id` | `lastStnId` | string? | 직전 정류소 |
| `next_stop_id` | `nextStId` | string? | 다음 정류소 (버스도착 `stId` 와 조인) |
| `turn_stop_id` | `trnstnid` | string? | 의미 **미확인** (노선 내 값이 모두 같음 → 회차지 정류소로 추정) |
| `last_st_tm` | `lastStTm` | long? | 의미 **미확인** |
| `next_st_tm` | `nextStTm` | long? | 의미 **미확인** |
| `sect_dist` | `sectDist` | double? | 의미 **미확인** |
| `full_sect_dist` | `fullSectDist` | double? | 의미 **미확인** |
| `rt_dist` | `rtDist` | double? | 의미 **미확인** (노선 내 값이 모두 같음 → 노선 총 거리로 추정) |
| `bus_type` | `busType` | long? | 차량 유형 코드 |
| `congestion` | `congetion` | long? | 혼잡도 코드로 보이나 값의 의미 **미확인**. API 철자(오타) 그대로 읽는다 |
| `stop_flag` | `stopFlag` | long? | 의미 **미확인** (0/1) |
| `is_running` | `isrunyn` | boolean? | 운행 여부. 예상 밖 값이면 None + `stats.unexpected_flag_values` |
| `is_last` | `islastyn` | boolean? | 막차 여부 |
| `is_full` | `isFullFlag` | boolean? | 만차 여부 |

## 측정 예정 / 미확인

- [x] 갱신 주기 측정 (60초·15초, 위 표). [ ] 한도가 공식 확인되면 간격 재검토
- [ ] 일/월 호출 한도 (data.go.kr 마이페이지 '활용신청 현황'에서 확인)
- [ ] '의미 미확인' 필드의 정의 — 서비스 활용가이드 문서 확인
- [ ] 0대 응답과 `headerCd` ≠ 0 응답의 모양
- [ ] 대상 노선 범위 (1차는 753번 하나, 노선 목록은 `config/sources.yaml` 에서 늘린다)


## 2026-11-04 추가: 결과 없음 응답
- 스모크 테스트에서 `headerCd=4` ("결과가 없습니다") 관측 (04시 직후). 이제 `empty` (정상, 재시도·서킷 실패 집계 없음). 원문 픽스처 `empty_code4_20261104T024101.xml` 는 **재구성본**이라 실제 응답과 태그가 다를 수 있다 → 실제 원문 확보 시 교체 (D-42).
