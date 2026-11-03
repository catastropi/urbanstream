# seoul_incident — 서울 실시간 돌발 정보

계약: `incident_v1` · 파서: `src/urbanstream/contracts/incident.py` · 샘플: `tests/fixtures/recorded/seoul_incident/`

## 개요

| 항목 | 내용 | 근거 |
|---|---|---|
| 공식명 / 서비스명 | 서울시 실시간 돌발 정보 / `AccInfo` | 호출 성공 |
| 제공기관 | 서울특별시 (서울 열린데이터광장) | 호출 URL |
| 쓰이는 문제 | B 돌발 영향 분석, A 혼잡 원인 설명 | Blueprint |
| 인증 | `SEOUL_API_KEY` | 호출 성공 |
| 호출 | `http://openapi.seoul.go.kr:8088/{KEY}/xml/AccInfo/1/50/` | 2026-11-01 18:07 PASS |
| 응답 형식 | **XML만 지원** | 2026-10-28 관측 |
| 호출 한도 / 갱신 주기 | 한도 **미확인** (이용약관 제6조에 숫자 없음; 수집기는 보수적으로 일 1,000건 상한). 갱신: 20분(2026-11-02 13:06~13:23, 1회) 동안 목록이 호출 20회 중 14번 바뀜 — 사건이 생기고 해제될 때마다 바뀌는 목록이라 소스 주기는 알 수 없음. 새로 나타남 6 / 사라짐 14, **같은 acc_id 의 필드 변경 0/452** (해제 예정 시각 포함) → content_hash 가 매 호출 새 버전을 만들지 않음(이 구간에서). 해제 시각은 필드가 아니라 '목록에서 사라진 첫 관측'일 가능성이 높음(미검증). 지연 평균 1.3초, p95 1.6초 | 2026-11-02 |
| 호출 지연 | 1,250 ms (1회 관측, 도로소통보다 느림) | 리포트 |
| 전체 건수 | `list_total_count` = 10, 1~50 요청에 10건 전부 수신 | 관측 |

## 관측한 내용 (2026-11-01 18:07 스냅샷, 10건)

| acc_type | 건수 | 의미(설명문에서 확인) |
|---|---|---|
| `A01` | 1 | 사고 (북부간선로 추돌사고, 발생 16:50) |
| `A04` | 5 | 공사·작업 (장기 공사는 해제 예정이 2027~2030년) |
| `A10` | 2 | 행사·통제 (차 없는 거리) |
| `A11` | 2 | 버스 우회, 노면 요철 등 기타 |

- 장기 공사가 '돌발' 목록에 섞여 온다. 분석에서 '지금 교통에 영향을 주는 돌발'만 쓰려면 Silver/Gold 에서 거른다 (계약은 거르지 않는다).
- `occr_time` 은 **4자리(HHMM)와 6자리(HHMMSS)가 섞여** 온다 (`1006`, `152200`). `exp_clr_time` 은 6자리.
- `acc_road_code` 가 **빈 문자열**로 오는 행이 있다 (10건 중 1건).
- `acc_info` 는 줄바꿈이 CR(`&#13;`)로 들어 있다.

## 시간·공간 정보

- 시간: 발생 `occr_date`+`occr_time`, 해제 예정 `exp_clr_date`+`exp_clr_time` (KST).
- 공간: `grs80tm_x/y` (TM 좌표, **EPSG:5181**) 와 `link_id`.
  - 검증: 버스위치의 gps↔pos 13쌍을 변환한 결과 EPSG:5181 과 최대 6 m 차이(다른 후보는 수백 m 이상). 돌발 10건은 EPSG:5181 → WGS84 변환 좌표가 설명문의 지명(봉은교, 소공로, 청계광장, 신촌 연세로, 올림픽대로 한강대교~동작대교, 북부간선로 하월곡)과 일치.
- `link_id` 가 도로소통의 `link_id` 와 같은 체계인지는 **미확인** (둘 다 10자리 숫자지만 조인 검증 전).

## 오류·장애

도로소통과 같은 서울 응답 형식이다 → `seoul_traffic.md` 의 오류 표 참고. (돌발 API 로도 `INFO-100`, `ERROR-301` 을 관측했다.)

## 계약 (incident_v1)

- event_time: occr_date + occr_time (KST, HHMM 또는 HHMMSS)
- dedup_key: acc_id | 내용해시 앞 8자 (해제시각·설명 등이 바뀌면 새 키)
- source_system: `seoul_incident`
- 같은 `acc_id` 의 내용이 바뀌면 새 레코드로 남는다(이력 보존). 최신 상태 선택은 Silver 의 일.

| payload 필드 | 원본 필드 | 타입 | 설명 |
|---|---|---|---|
| `acc_id` | `acc_id` | string | 돌발 ID (필수) |
| `occurred_at` | `occr_date`+`occr_time` | timestamp | 발생 시각 = envelope.event_time |
| `expected_clear_at` | `exp_clr_date`+`exp_clr_time` | timestamp? | 해제 예정. 날짜가 없으면 None, 시각이 없으면 00:00:00 |
| `acc_type` | `acc_type` | string | 대분류 (필수) |
| `acc_detail_type` | `acc_dtype` | string? | 세부 유형 |
| `link_id` | `link_id` | string? | 도로 링크 ID |
| `tm_x` | `grs80tm_x` | double? | TM 좌표 x (EPSG:5181) |
| `tm_y` | `grs80tm_y` | double? | TM 좌표 y (EPSG:5181) |
| `description` | `acc_info` | string? | 설명. CR/LF → `\n` |
| `road_code` | `acc_road_code` | string? | 빈 문자열은 None |
| `content_hash` | (파생) | string | 해제 시각·유형·링크·설명으로 만든 해시. 내용 변경 감지 |

## 측정 예정 / 미확인

- [x] 갱신 주기 1차 측정 (위 표). [ ] 하루치 원본으로 `analyze_smoke_raw.py incident` 를 다시 돌려 해제 예정 시각이 바뀌는 경우가 있는지 확인
- [ ] 사라진 돌발이 실제 해제인지 예정 시각 만료인지 (사라진 시각 vs `exp_clr_*` 비교)
- [ ] 호출 한도
- [ ] 해제된 돌발이 목록에서 사라지는 시점 (해제 시각 이후 얼마나 남아 있는지)
- [ ] `acc_type`/`acc_dtype` 전체 코드표 (관측: A01 A04 A10 A11 / 01B01 04B01 04B04 10B03 11B01)
- [ ] `link_id` ↔ 도로소통 `link_id` 조인 가능 여부
