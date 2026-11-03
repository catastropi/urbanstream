# bus_arrival — 서울 버스도착 (노선 전체 정류소)

계약: `bus_arrival_v1` · 파서: `src/urbanstream/contracts/bus_arrival.py` · 샘플: `tests/fixtures/recorded/bus_arrival/`

## 개요

| 항목 | 내용 | 근거 |
|---|---|---|
| 공식명 | 서울특별시_버스도착정보조회 서비스 (data.go.kr) | 승인 목록 |
| 서비스 / 오퍼레이션 | `ws.bus.go.kr` / `getArrInfoByRouteAll` | 호출 성공 |
| 쓰이는 문제 | C 대중교통 지연 탐지 | Blueprint |
| 인증 | `DATA_GO_KR_KEY` | 호출 성공 |
| 호출 | `http://ws.bus.go.kr/api/rest/arrive/getArrInfoByRouteAll?ServiceKey=...&busRouteId=100100118` | 2026-11-01 21:53 PASS |
| 응답 형식 | XML (`ServiceResult`) — `bus_position` 과 같은 껍데기 | 관측 |
| 호출 한도 | **일일 트래픽 1,000건** (사용자가 공공데이터포털 화면에서 확인, 2026-11-03; 상세기능별). 포털 호출 통계와 `collector status` 숫자를 비교해 같은 기능이 집계되는지 확인 필요 | 2026-11-03 |
| 갱신 주기 | **약 20초로 추정**. 20초 간격 14회에서 `mkTm` 이 14/14 바뀌고 값이 `14:40:02`, `14:40:19`, `14:40:39` (17~20초 간격), 저장 시각과의 차이는 2~20초(중앙값 8초). 응답 전체 해시는 카운트다운 필드 때문에 60초·20초 간격 모두 항상 바뀌어 주기를 말해 주지 않음. 지연 평균 0.9초(p95 2.7초, 60초 측정) / 0.13초(20초 측정). 수집은 90초(한도 가정 때문) → 갱신의 약 1/4~1/5만 봄 | 2026-11-03 |
| 호출 지연 / 크기 | 69 ms / **236,632 B** (104행) | 리포트 |
| 압축 | gzip -9 로 12,360 B (약 19배). 1분 주기 원본 gzip 보관 시 노선당 하루 약 17.8 MB | 직접 측정 |

## 응답 구조 (관측)

정류소 1행 = 노선 안의 정류소 하나(`staOrd` 순번). 그 행에 도착 예정 버스 **2대**가 접미사 `1`/`2` 로 붙는다
(`arrmsg1/2`, `vehId1/2`, `traTime1/2` ...). 753번은 104행.

- 버스가 없는 슬롯은 `vehId{n}` = `0`, `plainNo{n}` = `' '`(공백 1자). 샘플: 슬롯 208개 중 15개가 비어 있다.
- **응답 컬럼은 88개**다. 이 샘플에서 항상 0 인 컬럼은 10개(`avgCf`, `kalCf`, `neuCf`, `full` 등)이고, 값이 변하지만 의미를 확인하지 못한 컬럼이 약 45개(`nmain*`, `nstn*`, `goal`, `nextBus`, `routeType`, `term` 등)다. 상수 0 이라는 판단은 샘플 1건(막차 전 일요일 밤)이라 확정하지 않는다.
- 일부 행에만 있는 컬럼: `stationNm1/2`(1.9%/12.5% 결측), `repTm1`(7.7% 결측, 값이 2021년 → 신뢰 불가). 스키마 진화 대상.
- `dir` 은 공백 1자(전부 결측). `firstTm`/`lastTm` 은 14자리가 정상인데 첫 정류소 행은 **10자리(`2026110100`)로 깨져 온다** (1/104).
- 시간 필드 의미 (샘플로 확인): `traTime{n}` 은 `arrmsg{n}` 의 시간과 일치한다 (116 = '1분56초후', 207 = '3분27초후').
  → 도착까지 남은 초. `exps/kals/neus{n}` 은 같은 값을 다른 방식으로 예측한 초 단위 값으로 보이나 모델 이름은 **미확인**.
- `arrmsg` 형식: `1분56초후[1번째 전]`, `곧 도착`, `출발대기`.

## 시간·공간 정보

- 시간: `mkTm` ('2026-11-03 14:40:02.0', KST, **초 단위** — 서버가 약 20초마다 새 스냅샷을 만든다. 처음 샘플의 `:00.0` 은 우연).
- 공간: 좌표 없음. 정류소 ID(`stId`)는 버스위치의 `nextStId`/`lastStnId` 와 같은 체계 (12/12), 차량 ID(`vehId{n}`)는 버스위치 `vehId` 와 13/13 일치.

## 오류·장애

`bus_position.md` 와 같은 껍데기·같은 게이트웨이 → 같은 표.

## 계약 (bus_arrival_v1)

- event_time: mkTm (초 단위, KST)
- dedup_key: busRouteId | stId | staOrd | slot | YYYYMMDDHHMMSS
- source_system: `bus_arrival`
- 한 행을 (정류소, 슬롯) 레코드로 펼친다. 빈 슬롯은 레코드도 reject 도 아니고 `stats["empty_slots"]` 로 센다.
- 0 을 결측으로 바꾸지 않는다 (`traTime` 0 은 '곧 도착'일 수 있다). 해석은 Silver.
- 샘플: 레코드 193 = 슬롯 208 − 빈 슬롯 15. dedup_key 모두 유일.

| payload 필드 | 원본 필드 | 타입 | 설명 |
|---|---|---|---|
| `route_id` | `busRouteId` | string | 노선 ID (필수) |
| `route_name` | `rtNm` | string? | 노선 번호 (예: 753) |
| `station_id` | `stId` | string | 정류소 ID (필수) |
| `station_ars_id` | `arsId` | string? | 정류소 고유번호 |
| `station_name` | `stNm` | string? | 정류소 이름 |
| `station_order` | `staOrd` | long | 노선 안의 정류소 순번 (필수) |
| `slot` | (접미사) | long | 1 = 첫 번째 도착 버스, 2 = 두 번째 |
| `veh_id` | `vehId{n}` | string | 차량 ID (`0` 이면 빈 슬롯) |
| `plain_no` | `plainNo{n}` | string? | 번호판, 공백 제거 |
| `arrival_msg` | `arrmsg{n}` | string? | 도착 안내 문구 |
| `stops_away` | (파생) | long? | `arrival_msg` 의 `[N번째 전]` 에서 뽑은 N. 없으면 None |
| `arrive_in_sec` | `traTime{n}` | long? | 도착까지 남은 초 |
| `pred_exps_sec` | `exps{n}` | long? | 예측값(초) |
| `pred_kals_sec` | `kals{n}` | long? | 예측값(초) |
| `pred_neus_sec` | `neus{n}` | long? | 예측값(초) |
| `speed` | `traSpd{n}` | long? | 속도로 보이나 단위 **미확인** (관측 14~26, 출발대기 행에서 154 관측 → 의미 재확인 필요) |
| `arrive_code` | `isArrive{n}` | long? | **불리언이 아니라 코드값.** 0 과 2 관측 — 2 는 `arrmsg` 가 '출발대기'인 노선 시작점 행들(2026-11-02 12:31). 코드표 **미확인** |
| `is_last_bus` | `isLast{n}` | boolean? | 막차 여부. 예상 밖 값이면 None (행은 유지, `stats.unexpected_flag_values` 로 집계) |
| `bus_type` | `busType{n}` | long? | 차량 유형 코드 |
| `bus_station_name` | `stationNm{n}` | string? | 버스가 있는 정류소 이름으로 추정 (일부 행에만 존재) |
| `first_bus_time` | `firstTm` | timestamp? | 첫차 시각. 형식이 깨지면 None |
| `last_bus_time` | `lastTm` | timestamp? | 막차 시각. 형식이 깨지면 None |

payload 에서 뺀 것: 위의 상수 0 컬럼 10개, 의미 미확인 컬럼 약 45개, `repTm1`, `dir`. 원본은 Bronze 에 그대로 남으므로 의미를 확인하면 v2 에서 추가한다.

## 측정 예정 / 미확인

- [x] 갱신 주기 — 20초 간격에서 `mkTm` 14/14 변경, 값은 17~20초 간격 (2026-11-03). 서버가 약 20초마다 갱신(추정)
- [ ] 호출 한도
- [ ] `exps`/`kals`/`neus` 모델 이름, `traSpd` 단위, `isArrive` 코드표 (0/2 외 값이 있는지)
- [ ] 의미 미확인 컬럼 약 45개 (`nmain*`, `nstn*` 는 다음 차량 정보로 보이나 추정) — 활용가이드 확인 후 v2 에서 채택 여부 결정
- [ ] 막차 이후·심야의 응답 모양 (이번 샘플은 막차 전, 21:53)
- [ ] 한 호출 236 KB 를 Bronze 에서 어떻게 보관할지 — Step 3 설계 (원본 gzip 보관이 1차 안)
