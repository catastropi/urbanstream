# Step 10 런북 — 대시보드(Streamlit)와 Tableau CSV

결정 근거: `docs/adr/0012-step10-dashboard.md`. 남은 것: `docs/DEFERRED.md` J장.
**사용자가 Mac 에서 실행하는 순서입니다.** 명령은 한 줄씩이고, 비밀 값(`.env` 의 `DASHBOARD_API_KEY`, `API_KEYS`)은 **채팅·git 에 붙이지 마세요.**
상태 표기: 아래 "기대 결과"는 모두 **[Mac 미확인]** 입니다(샌드박스에서는 가짜 API 로 화면·시험만 확인).

## 0. 압축 풀기

```zsh
cp ~/Downloads/urbanstream_full.zip .
```

```zsh
unzip -o urbanstream_full.zip -d .
```

`.env` 와 `data/` 는 zip 에 없으므로 건드리지 않습니다. 이번에 바뀐 것: `dashboard/`(새), `docker/dashboard/`(새), `requirements-dashboard.txt`(새), `docker-compose.yml`(서비스 `dashboard`), `scripts/stack.sh`(`up-dashboard`), `scripts/stack_env.py`·`.env.example`(대시보드 키), `scripts/export_tableau.py`·`scripts/mutation_dashboard.py`(새), `tableau/README.md`, `.gitignore`.

## 1. `.env` 에 대시보드 키 만들기

```zsh
scripts/stack.sh init-env
```

기대 결과: "추가한 키: DASHBOARD_PORT, DASHBOARD_API_KEY" 와 "빈 값을 채운 키: API_KEYS(대시보드 키 추가)" 처럼 **이름만** 나옵니다. 기존 `API_KEYS` 값은 그대로이고 끝에 `,새키` 가 붙습니다. 한 번 더 실행하면 "변경 없음" 이어야 합니다.

## 1a. (참고) 버스 노선

버스 API 는 노선 ID 가 필수입니다. 기본값은 753번(`100100118`)이고, `.env` 에 `SEOUL_BUS_ROUTE_ID` 가 있으면 그 값을 대시보드와 내보내기가 씁니다(비밀 아님). 대시보드만 따로 바꾸려면 `.env` 에 `DASHBOARD_BUS_ROUTE_ID=…`.

## 2. 기동

```zsh
scripts/stack.sh up-dashboard
```

postgres → 마이그레이션 → `api_service` 비밀번호 → api + dashboard 빌드·기동(첫 빌드는 몇 분). 기대 결과: `ps` 에 `urbanstream-api`, `urbanstream-dashboard` 가 healthy.

## 3. 화면 확인

브라우저에서 `http://127.0.0.1:8501` 을 엽니다.

| 화면 | 질문 → 결론 | 확인할 것 |
|---|---|---|
| 개요 | 파이프라인이 도는가 | 전체 상태 · 소스별 마지막 정상 시각 |
| 지도 | 어디가 막히고 돌발·버스는? | 링크 4개 회색(= 판정 불가)이 정상(표본 부족). 바탕 지도는 **브라우저가 인터넷**으로 받음 |
| 혼잡(A) | 평소보다 얼마나 막히나 | 단계가 "판정 불가"로 나오는 것이 현재 정상 |
| 돌발 영향(B) | 돌발이 속도에 준 영향 | 해제 후 24시간 전은 잠정 |
| 버스(C) | 예측 도착이 얼마나 밀리나 | 시간표 지연 아님 |
| 날씨(D) | 비 올 때 속도 비율 | 연관, 인과 아님. 표본 부족 구간은 해석 금지 |
| 이상·건강(E·F) | 평소와 다른 값 / 어디서 멈췄나 | 후보(표본 부족)는 이상이 아님 |

이상·건강을 뺀 6개 화면 위쪽에 "'현재' 값은 실시간이 아니다(최대 약 80분)" 안내가 보여야 합니다. 사이드바의 "API 호출(최근 1분)"은 안정 상태에서 약 12회 이하여야 합니다(서버 한도 120).

## 4. 메모리 확인 (사용자)

```zsh
scripts/stack.sh stats
```

`urbanstream-dashboard` 의 메모리가 한도(기본 512m) 안인지 봅니다. 한도에 가까우면 `.env` 에 `DASHBOARD_MEM_LIMIT=768m` 처럼 적고 다시 `up-dashboard` 합니다.

## 5. Tableau CSV 만들기

```zsh
python scripts/export_tableau.py --preview
```

```zsh
python scripts/export_tableau.py
```

`tableau/export/` 에 CSV 5개와 `manifest.json` 이 생깁니다(git 제외). **Tableau Public 에 올리면 누구나 볼 수 있습니다.** `manifest.json` 의 `publish_checklist` 를 읽고, 원천 데이터 이용 조건과 Tableau Public 약관을 **직접 확인한 뒤** 직접 올리세요. 스크립트는 업로드하지 않습니다.

### 5a. Tableau Public 설치·게시 (사용자 작업, 처음 한 번)
1. `public.tableau.com` 에서 무료 계정을 만든다. 설치 없이 웹 작성(Create → Web Authoring)도 가능하고, 오프라인 작업은 *Tableau Desktop Public Edition* 을 받는다(이름·이메일·국가 입력 후 다운로드 → macOS 는 dmg 를 열어 Applications 로 끌어넣기). 둘 다 무료.
2. 앱에서 *Text file* 로 `tableau/export/*.csv` 를 연결한다(한글은 utf-8-sig 라 깨지지 않는다). CSV 5개는 서로 다른 표 → 시트별로 하나씩 연결.
3. `manifest.json` 의 `publish_checklist` 를 먼저 읽는다. `provisional=true` 열은 화면에 "잠정"이라고 표시하고, 날씨 효과가 비어 있으면 '표본 부족'으로 둔다.
4. **Tableau Public 에 게시하면 누구나 볼 수 있고 데이터도 내려받을 수 있다.** 서울시 데이터 이용약관·Tableau Public 약관을 확인한 뒤에만 게시한다(D-165, D-170). 확신이 없으면 게시하지 말고 저장만 한다(Public Edition 은 로컬 저장 가능 여부가 버전마다 다르니 [미확인]).
5. 자동 갱신은 없다: CSV 를 다시 만들어 데이터 원본을 새로 고치고 다시 게시한다.

## 6. 문제가 생기면

| 증상 | 원인 / 조치 |
|---|---|
| `up-dashboard` 가 ".env 에 DASHBOARD_API_KEY…" | `scripts/stack.sh init-env` 먼저 |
| 화면에 "API 키가 거부되었습니다" | `DASHBOARD_API_KEY` 가 `API_KEYS` 에 없음 → `init-env` → API 컨테이너 재시작(`scripts/stack.sh up-dashboard`) |
| "갱신 실패 — … 에 받은 값을 보여 주는 중" | API 일시 오류. 옛 값을 표시 중이며 시각이 적혀 있음 |
| 지도에 점·선은 있고 바탕이 비어 있음 | 브라우저가 지도 타일을 받지 못함(인터넷). 데이터 문제 아님 |
| 컨테이너가 계속 재시작 | `scripts/stack.sh logs dashboard` — 키 비어 있음/API 미기동 |

## 7. 테스트 (사용자 Mac, 선택)

```zsh
python -m pytest -q tests/dashboard tests/unit
```

```zsh
python scripts/mutation_dashboard.py
```

기대 결과: 변이 31건 모두 "잡힘". 대시보드 시험은 `pip install -r requirements-dashboard.txt` 가 있어야 돕니다 — 없으면 `tests/dashboard` 화면 시험 2개는 **건너뜀**으로 표시됩니다(오류 아님). `tests/api` 도 `fastapi` 가 없으면 건너뜁니다(`pip install -r requirements-api.txt`).
