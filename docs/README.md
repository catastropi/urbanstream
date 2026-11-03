# docs

- `step0-guide.md` — Step 0 (API 스모크 테스트) 실행 가이드와 확정 사항.
- `api-catalog/` — Step 0 에서 **직접 호출해 확인한** 각 API 의 필드·코드·형식·계약 기록. 확인 안 된 칸은 '미확인'. 입구는 `api-catalog/README.md`.
- `DEFERRED.md` — **안 한 것·확인 못 한 것·일부러 미룬 것**과 어디에 추가할지의 단일 장부. 새 채팅은 HANDOFF.md 와 함께 읽는다.
- `adr/` — Architecture Decision Record. 구조적 결정을 날짜·이유와 함께 한 파일씩 남긴다.
  - `0001-envelope-event-time-dedup.md` — envelope 필드, 소스별 event_time / dedup_key
  - `0002-contracts-stdlib-dataclasses.md` — 계약 코드에 외부 검증 라이브러리를 쓰지 않는 이유
  - `0003-compose-infra.md` — Docker 인프라의 범위·이미지·포트·메모리 결정 (실측 반영)
  - `0004-collector-topics.md` — Kafka 토픽·메시지 형식·Collector 설계 (재시도·회로차단·예산)
  - `0005-bronze-spark.md` — Step 4 Bronze: Spark 버전·이미지, Parquet, 메모리 구성, 쓰기 방식 (+ 정정 1: 실측 반영)
  - `0006-step5-pre-decisions.md` — Step 5 시작 전 결정: Silver 입력, Delta 보류, 과거 구간, 돌발 처리
  - `0007-silver-rules.md` — Silver 규칙: 워터마크, 0값/결측, 장기 돌발(7일), 기상 긴 형태 (5-0 실측 근거)
- `runbooks/` — 절차서.
  - `step0-closeout.md` — Step 0 마무리 측정·확인 (사용자가 Mac 에서 실행)
  - `step2-stack.md` — Docker 스택 기동·점검·메모리 측정 (사용자가 Mac 에서 실행)
  - `step3-collector.md` — 토픽 만들기와 Collector 첫 실행·운영 (사용자가 Mac 에서 실행)
  - `step4-bronze.md` — Bronze(Spark) 첫 실행·검증·복구 (사용자가 Mac 에서 실행)
  - `step5-silver.md` — Silver 준비: 5-0 Bronze 프로파일링 (진행 중)
  - 장애 대응 런북(Blueprint U절)은 Collector 이후에 작성.

전체 설계는 이 저장소 밖(대화 세션)에서 만든 Architecture Blueprint 문서가 기준이며,
이 폴더는 그 설계가 실제 구현·관측과 만나면서 구체화되는 기록을 쌓는 곳이다.
**충돌하면 Step 0 에서 직접 확인한 사실이 Blueprint 보다 우선한다.**
