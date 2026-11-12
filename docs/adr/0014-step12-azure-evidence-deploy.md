# ADR-0014 — Step 12: Azure 최소 증거 배포 (서빙 계층만 올리고 destroy)

날짜: 2026-11-12. 상태: 구현됨(샌드박스: 정적·단위 시험 + 변이 시험) · **`terraform apply`·VM 기동·접속은 한 번도 실행하지 못함** — 사용자가 `docs/runbooks/step12-cloud.md` 를 실행해야 확인됨(`docs/DEFERRED.md` O장).
사용자 결정: 2026-11-11 "24/7 운영은 하지 않고 'Azure 를 써 봤다'는 증거만 남기는 최소 배포 후 destroy", 2026-11-12 "기존 계획대로 진행"(범위 API·대시보드, Olist 유지, VM 빌드, B2s_v2 — 제안서 추천안).
사실 표기: **[샌드박스]** 이 환경에서 확인 / **[Azure 미확인]** 사용자 Azure 에서만 확인 가능 / **[추정]** 근거 없이 합리적으로 짐작.

## 맥락
스트리밍·배치(Kafka·Spark·Airflow·수집기)는 Mac 에서 돈다. Mac 절전이 장애의 근본 원인이었지만(D-222), 학생 구독 크레딧(₩101,300 남음, D-229)으로 24시간 가동은 약 2~3주가 한계이고 동시 vCPU 는 6이다(D-224·D-228). 그래서 서빙 계층(DB·API·대시보드)만 Azure 에 올려 코드로 재현되는 배포와 접속 화면·비용 기록을 증거로 남기고 지운다.

## 결정

| ID | 결정 | 이유 | 한계 / 되돌리기 |
|---|---|---|---|
| D12-1 | VM 1대 + docker compose. 관리형 서비스로 재구성하지 않는다 | 기존 코드를 안 바꾼다 | `terraform destroy` |
| D12-2 | `Standard_B2s_v2`(2 vCPU/8 GiB), Korea Central, Ubuntu 24.04 LTS, 별도 리소스 그룹 `urbanstream-step12-rg` | 서빙 계층 Mac 실측 합계 약 0.4 GiB(postgres 242 + api 51 + dashboard 128 MiB, D-216). 리소스 그룹을 따로 두면 Olist(D-227)를 건드리지 않고 destroy 확인이 쉽다 | 변수 `vm_size` 한 줄. B 계열은 버스터블 [추정] — 부하는 `docker stats` 로 확인. 이 크기·Gen2 이미지 조합의 실제 생성 [Azure 미확인] |
| D12-3 | Terraform(azurerm `~> 4.0`), state 로컬. 구독 ID 는 `ARM_SUBSCRIPTION_ID` 환경 변수로만 | 확정된 방침. 구독 ID·IP 를 코드·tfvars 에 쓰지 않는다 | `*.tfstate`·`*.tfvars`·`.terraform/` 는 `.gitignore` + `tracked` 시험이 막는다. `.terraform.lock.hcl` 은 커밋(provider 고정). state 분실 시 포털에서 리소스 그룹을 지우면 된다 |
| D12-4 | NSG 인바운드는 SSH(22) 한 개, 출발지는 `allowed_ssh_cidr`(필수, 기본값 없음, `/24` 이상·0.0.0.0/0 거부). 비밀번호 로그인 끔. 8010·8501 은 열지 않는다 | 확정된 방침(SSH 터널, 공개 안 함). 대시보드에 로그인이 없는 문제(D-199)를 피한다 | 내 IP 가 바뀌면 tfvars 수정 후 apply |
| D12-5 | compose 는 서비스 이름을 직접 지정(`postgres api dashboard`)해 올린다. `docker-compose.cloud.yml` 은 세 서비스의 `restart: unless-stopped` 만 덮어쓴다 | `core` 프로파일에 kafka 가 같이 있지만 이름을 지정하면 올라오지 않는다 [샌드박스: 병합 config 검증, 포트는 전부 127.0.0.1]. 프로파일 덮어쓰기 문법에 기대지 않는다 | override 삭제 |
| D12-6 | 이미지는 **VM 에서 빌드**한다. 소스는 Mac 에서 `git ls-files --cached --others --exclude-standard` 목록으로 tar 해 보낸다(`.gitignore` 에 걸린 `.env`·`data/`·state·tfvars 는 빠지고, 목록에 섞이면 중단) | GHCR 이미지는 private 이라 VM 에 `read:packages` 토큰(장기 비밀)이 필요하고 저장소도 private 이라 clone 에도 토큰이 필요. 태그도 다르다(`urbanstream/api:1.0.0` vs GHCR `0.1.0`). Mac 빌드 api 24초·dashboard 78초 | GHCR 방식은 override 의 `image:` 변경으로 가능 |
| D12-7 | 호스트 Python 의존(D-207)은 일회성 컨테이너로 푼다: api 이미지(psycopg 포함)에 소스를 `:ro` 로 마운트해 `migrate.py`·`db_bootstrap.py --api` 실행 | VM 에 conda·venv 를 만들지 않는다. 빈 DB 에 001~014 를 `migrate.py` 로 적용하는 첫 경로(D-202)가 실제로 검증된다 | 마이그레이션 SQL 은 수정하지 않았다 |
| D12-8 | 데이터는 Mac 에서 `pg_dump --data-only --disable-triggers`(스키마 dim·fact·mart·ops, `ops.schema_migrations` 데이터 제외)로 뜨고, VM 에서 마이그레이션 후 해당 표들을 `TRUNCATE ... CASCADE` 한 다음 **한 트랜잭션**으로 복원 | 스키마는 마이그레이션이 만든다(이력과 어긋나지 않음). 마이그레이션이 넣은 시드 행과 충돌하지 않게 먼저 비운다. 실패하면 반쪽 데이터가 남지 않는다 | 덤프 크기·복원 시간·FK 순서 [Azure 미확인]. 스냅샷이라 시간이 멈춰 API 의 `stale` 이 곧 true 가 된다 → 증거에 "스냅샷, 촬영 시각" 을 적는다 |
| D12-9 | CI: 새 `terraform` 잡 = `fmt -check` + `init -backend=false` + `validate` 만(apply/plan 안 함). 바이너리는 `scripts/ci_terraform.py` 가 `.github/terraform.pin`(버전·sha256)으로 받아 검증한다. 핀이 없으면 실패(fail-closed) | `hashicorp/setup-terraform` 은 서드파티라 워크플로 규칙(1st-party 만)에 걸린다. gitleaks 와 같은 방식 | 🔴 핀은 사용자가 `python scripts/pin_terraform.py` 로 만들어 커밋해야 한다(샌드박스는 releases.hashicorp.com 에 접근할 수 없었다). 그 전에는 `terraform` 잡이 빨간불 — 의도된 동작 |
| D12-10 | `deploy.yml`·Azure OIDC 는 만들지 않는다. 배포는 Mac 에서 수동(`scripts/cloud.sh`) | 학교 테넌트 앱 등록 권한 미확인(D-226). 워크플로 시험 규칙(비밀 제한·권한)을 건드릴 필요가 없다 | 필요해지면 별도 ADR + 시험 규칙 예외 |
| D12-11 | 터널은 Mac 로컬 포트 18010·18501(VM 8010·8501)로 연다 | Mac 로컬 스택이 8010·8501 을 쓰고 있을 수 있다 | `cloud.sh` 의 상수 |
| D12-12 | 시험·변이: `tests/unit/test_cloud_infra.py` 29개, `mutation_ci` 26건 추가(NSG 개방·비밀번호 로그인·ports 재정의·kafka 유입·터널 바인딩·전송 목록 검증·복원 원자성·키 출력·tfstate 추적·CI 핀 검증·서드파티 액션 등) | 안전장치를 일부러 깨서 시험이 잡는지 본다 | `terraform validate` 자체는 샌드박스에 바이너리가 없어 못 돌렸다(`python-hcl2` 로 구문만 확인) — CI 의 `terraform` 잡이 첫 검증이다 |

## 이번 범위에서 하지 않은 것
Kafka·Spark·Airflow·monitor·수집기 배포, 일일 백업(D12-8 구안), 수집기 컨테이너화(D-206), Airflow 안정화·컨테이너 종료 감시(D12-15·16; Mac 쪽 교훈 D-211~D-222 는 유지), 도메인·TLS·프록시, Key Vault, Azure OIDC 배포 워크플로, D-196(클라우드 IP 에서 공공 API 호출 — 키를 VM 에 넣어야 해서 새 키를 쓸 때만).

## 호환
`docker-compose.yml`·SQL 마이그레이션·OpenAPI 스냅샷·기존 시험 불변(기존 시험 파일은 `ci_changes` 그룹 수에 관한 기대값 2개만 `infra` 키 추가로 고쳤다). `ci_checks.py compose` 가 override 병합 결과도 검증한다.
