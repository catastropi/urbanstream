# docs

- `step0-guide.md` — Step 0 (API 스모크 테스트) 실행 가이드와 확정 사항.
- `api-catalog/` — Step 0 에서 **직접 호출해 확인한** 각 API 의 필드·코드·형식·계약 기록. 확인 안 된 칸은 '미확인'. 입구는 `api-catalog/README.md`.
- `DEFERRED.md` — **안 한 것·확인 못 한 것·일부러 미룬 것**과 어디에 추가할지의 단일 장부.
- `adr/` — Architecture Decision Record. 구조적 결정을 날짜·이유와 함께 한 파일씩 남긴다.
  - `0001-envelope-event-time-dedup.md` — envelope 필드, 소스별 event_time / dedup_key
  - `0002-contracts-stdlib-dataclasses.md` — 계약 코드에 외부 검증 라이브러리를 쓰지 않는 이유
  - `0003-compose-infra.md` — Docker 인프라의 범위·이미지·포트·메모리 결정 (실측 반영)
  - `0004-collector-topics.md` — Kafka 토픽·메시지 형식·Collector 설계 (재시도·회로차단·예산)
  - `0005-bronze-spark.md` — Step 4 Bronze: Spark 버전·이미지, Parquet, 메모리 구성, 쓰기 방식 (+ 정정 1: 실측 반영)
  - `0006-step5-pre-decisions.md` — Step 5 시작 전 결정: Silver 입력, Delta 보류, 과거 구간, 돌발 처리
  - `0007-silver-rules.md` — Silver 규칙: 워터마크, 0값/결측, 장기 돌발(7일), 기상 긴 형태 (5-0 실측 근거)
  - `0008-gold-postgres.md` · `0009-step7-airflow.md` · `0010-step8-dq-monitoring.md` — Gold(Postgres) / Airflow·마트 / DQ·모니터링·알림
  - `0011-step9-api.md` — Step 9 FastAPI: 로그인 역할(013), 인증, 응답 봉투(as_of/stale), 페이지네이션·기간, 오류 형식, 캐시·속도 제한, 컨테이너, 성능 결정, 엔드포인트↔원천 표
  - `0012-step10-dashboard.md` — Step 10 대시보드(Streamlit)·API 전용 입구·대시보드 키 분리·호출 예산·정직한 표시 규칙·컨테이너·Tableau CSV(옵션 A)
  - `0013-step11-ci-cd.md` — Step 11 시험 계층 마커·GitHub Actions(CI·주간·릴리스)·비밀 스캔·GHCR·비용
  - `0014-step12-azure-evidence-deploy.md` — Step 12 Azure 최소 증거 배포: VM 1대·NSG SSH 전용·compose 오버라이드·스냅샷 복원·터널·destroy (D12-1~12)
- `evidence/step12/` — Step 12 증거 이미지 19장(마스킹함)과 설명 `README.md` (Azure 배포·접속·비용·파괴).
- `runbooks/` — 절차서.
  - `step0-closeout.md` — Step 0 마무리 측정·확인 (사용자가 Mac 에서 실행)
  - `step2-stack.md` — Docker 스택 기동·점검·메모리 측정 (사용자가 Mac 에서 실행)
  - `step3-collector.md` — 토픽 만들기와 Collector 첫 실행·운영 (사용자가 Mac 에서 실행)
  - `step4-bronze.md` — Bronze(Spark) 첫 실행·검증·복구 (사용자가 Mac 에서 실행)
  - `step5-silver.md` — Silver 준비: 5-0 Bronze 프로파일링 (진행 중)
  - `step6-gold.md` · `step7a-airflow.md` · `step7b-marts.md` · `step7c-marts.md` · `step8-recovery.md` — Gold / Airflow / 마트 / 복구 훈련
  - `step9-api.md` — API 기동·점검(curl)·오류 코드·테스트·키 교체 (사용자가 Mac 에서 실행)
  - `step10-dashboard.md` — 대시보드 기동·화면 확인·메모리·Tableau CSV 만들기와 게시 전 점검 (사용자가 Mac 에서 실행)
  - `mac-on-off.md` — Mac 에서 스택·수집기를 끄고 켜는 절차, 절전(덮개 닫힘) 주의 (사용자가 Mac 에서 실행, Step 12 전)
  - `step11-ci.md` — 핀 만들기(gitleaks·액션 SHA)·commit/push·Actions 첫 실행 확인·릴리스 태그·되돌리기 (사용자가 Mac 에서 실행)
  - `step12-cloud.md` — Step 12 코드 완료 시점(11-12)의 실행 절차서(apply→up→터널→destroy). 실제 실행 결과 기반 절차는 `step12-azure.md`
  - `step12-azure.md` — Azure 서빙 계층 만들기(Cloud Shell `terraform apply`)·올리기(`cloud.sh up`)·터널·지우기·막혔을 때 표 (시연 부활 절차, 사용자가 실행)

전체 설계는 이 저장소 밖(대화 세션)에서 만든 Architecture Blueprint 문서가 기준이며,
이 폴더는 그 설계가 실제 구현·관측과 만나면서 구체화되는 기록을 쌓는 곳이다.
**충돌하면 Step 0 에서 직접 확인한 사실이 Blueprint 보다 우선한다.**
