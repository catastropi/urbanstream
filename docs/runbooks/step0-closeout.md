# Step 0 마무리 런북 — 사용자가 직접 해야 하는 측정과 확인

이 항목들은 서울/공공데이터 API 를 **사용자의 Mac 에서 호출해야** 하거나, 사이트에 로그인해서 봐야 해서 Claude 쪽 환경에서는 할 수 없다.
결과를 채팅에 붙여넣으면 Claude 가 `docs/api-catalog/` 의 '미확인' 칸을 채운다.
**키 값은 붙여넣지 않는다.** 리포트와 출력의 키는 `***KEY***` 로 마스킹돼 있다.

모든 명령은 프로젝트 루트(`urbanstream/`)에서, `urbanstream` conda 환경(Python 3.11)으로 실행한다.

```zsh
conda activate urbanstream
python --version     # 3.11.x 인지 확인 (base 로 실행하지 않기)
```

## 1. 갱신 주기 측정 (`cadence`) — 평일 낮, 터미널 4개 동시

언제: **평일 07:30~09:30(출근 시간대)이 가장 좋다.** 도로·버스는 한가한 시간에는 값이 덜 바뀌어서 주기를 잘못 읽을 수 있다. (일요일 심야 측정은 피한다.)

터미널을 4개 열고 각각 실행한다 (각 20분, 1분 간격 약 21회 호출. 버스 개발계정 일 1,000건 가정 대비 충분히 작다):

```zsh
python scripts/smoke_test.py cadence --api seoul_traffic  --interval 60 --minutes 20
python scripts/smoke_test.py cadence --api seoul_incident --interval 60 --minutes 20
python scripts/smoke_test.py cadence --api bus_position   --interval 60 --minutes 20
python scripts/smoke_test.py cadence --api bus_arrival    --interval 60 --minutes 20
```

- 버스위치는 차량 보고 주기가 수초~수십 초일 수 있어 1분 간격이면 해상도가 부족하다. 위 4개를 끝낸 뒤 한 번 더: `--api bus_position --interval 15 --minutes 10` (약 41회).
- 결과: `data/smoke/cadence_<api>_<시각>.md` 4~5개. **파일 내용을 붙여넣어 주세요.**
- 의심스러운 결과(변경 횟수 부족 등)가 나오면 `--minutes 30` 으로 늘려 한 번만 다시 한다.

## 2. 기상청 제공 시각 (`kma-lag`) — 아무 때나, 약 1시간

```zsh
python scripts/smoke_test.py kma-lag --wait-top
```

다음 정각까지 기다렸다가 1분마다 조회해서 처음 성공하는 시각을 기록한다. 결과 `data/smoke/kma_lag_<시각>.md` 를 붙여넣는다.

## 3. 호출 한도 확인 (사이트에서 직접)

문의 없이 화면에서 확인하는 방법만 쓴다. 화면 구성은 바뀔 수 있어서 메뉴 이름은 정확하지 않을 수 있다.

| 대상 | 어디서 | 적어 올 것 |
|---|---|---|
| 서울 버스 위치/도착 (data.go.kr) | 공공데이터포털 로그인 → 마이페이지 → 개발계정/활용신청 현황 → 해당 서비스 상세의 '트래픽' 항목 | 서비스별 일 트래픽 숫자 |
| 서울 도로소통/돌발 (열린데이터광장) | data.seoul.go.kr 로그인 → 마이페이지의 인증키 관리/이용 현황 화면, 또는 데이터셋 상세 페이지의 안내 문구 | 일/월 한도 문구. 화면에 없으면 '화면에 없음' 이라고만 알려 주세요 |
| 기상청 | 이미 확인됨 (일 10,000건) | — |

화면에서 못 찾으면 문의하지 않고 넘어간다. 그 경우 Collector 는 보수적인 기본값(Step 3 에서 만들 `config/sources.yaml` 의 한도)으로 시작하고, 한도 오류가 처음 관측되면 그 응답으로 확정한다.

## 4. 도로 링크 지오메트리 확보

필요한 이유: 도로소통·돌발은 `link_id` 만 주고 좌표가 없다. 링크를 지도 위의 선으로 만들어야 버스·돌발을 도로 구간과 묶고 혼잡을 지도에 그린다 (문제 A, B, C).

1. data.seoul.go.kr 에서 '소통 돌발 도로별 링크 정보'(또는 '링크' + '지오메트리/좌표' 로 검색) 데이터셋을 찾는다.
2. 파일(CSV/SHP 등)로 내려받아 `data/reference/` 에 둔다. (`data/` 아래는 git 에서 제외된다)
3. 채팅에 알려 줄 것: 데이터셋 이름, 파일 형식, **컬럼 이름 목록과 첫 3행**, 전체 행 수. 파일 자체가 크면 위 정보만 있어도 된다.
4. 이어서 확인할 것: 도로소통 샘플 링크 `1220003800` 이 이 파일에 있는지, 돌발의 `link_id` 가 같은 파일에 있는지.

## 5. 노선·정류소 조회 서비스 승인 확인

```zsh
python scripts/smoke_test.py seoul-bus-ids --route-no 753
```

- 2026-11-01 18:09 까지는 두 조회가 모두 `401 등록되지 않은 서비스키` 였다 (미승인).
- 승인이 나면 같은 명령으로 성공한다. **승인 전이어도 1차 구현에는 지장이 없다** (753번 `busRouteId` 를 `.env` 에 직접 넣었다). 노선을 늘릴 때 필요하다.

## 6. 마치고 나서

- 위 결과를 채팅에 붙여넣는다 → Claude 가 `docs/api-catalog/*.md` 의 '갱신 주기', '호출 한도' 칸과 `config/sources.yaml` 초안을 채운다.
- 이 문서의 체크박스는 직접 표시해 둔다:
  - [ ] cadence 4종(+버스위치 15초)
  - [ ] kma-lag
  - [ ] 서울 버스 한도
  - [ ] 서울 열린데이터광장 한도
  - [ ] 링크 지오메트리 파일
  - [ ] 노선·정류소 서비스 승인 재확인
