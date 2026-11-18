# Step 12 런북 — Azure 최소 증거 배포 (apply → 시연 → destroy)

> **상태(2026-11-18): 11-12 코드 완료 시점의 절차서입니다.** 아래 "기대 결과"와 `[Azure 미확인]` 표기는 실행 전 기준이며 그대로 둡니다. 실제 실행에서 확인된 경로(Cloud Shell 의 `terraform apply`, MFA 우회)와 부활 절차는 `step12-azure.md`, 증거는 `docs/evidence/step12/`, 결과는 `docs/DEFERRED.md` P장(D-250~D-254)을 보세요.

결정 근거: `docs/adr/0014-step12-azure-evidence-deploy.md`. 남은 것: `docs/DEFERRED.md` L장.
**사용자가 Mac 에서 실행하는 순서입니다.** 명령은 한 줄씩이고, 구독 ID·키·이메일·내 IP·tfvars·state 는 채팅·git 어디에도 붙이지 않습니다.
상태 표기: 아래 "기대 결과"는 모두 **[Azure 미확인]** 입니다(샌드박스는 Azure 에 접근할 수 없어 Terraform 문법·정적 규칙·스크립트 구문만 확인했습니다).

**무엇을 올리나:** VM 1대(Korea Central, `Standard_B2s_v2`) 안의 postgres · api · dashboard. Kafka·Spark·Airflow·monitor·수집기는 올리지 않습니다(그래서 외부 API 키가 VM 에 필요 없습니다). 데이터는 Mac 마트의 **스냅샷**이라 시간이 멈춰 있습니다.
**비용:** `B2s_v2` 단가는 Korea Central 기준 [미확인](East US `B4s_v2` $0.166/h 의 약 절반 [추정]). 켜 둔 시간 + 디스크 + 고정 공용 IP 가 나갑니다. **끝나면 stop 이 아니라 destroy** 합니다.

## 0. 코드 적용 + CI (먼저, 한 번)

```zsh
cp ~/Downloads/urbanstream_full.zip .
```

```zsh
unzip -o urbanstream_full.zip -d .
```

```zsh
rm urbanstream_full.zip
```

zip 을 풀면 워크플로가 `@vN` 태그로 돌아가므로 SHA 고정을 다시 합니다.

```zsh
python scripts/pin_actions.py
```

CI 의 새 `terraform` 잡은 Terraform 바이너리를 **체크섬 고정**으로 받습니다(서드파티 액션 금지 규칙). 핀을 만들어 커밋해야 초록불이 됩니다(없으면 빨간불 — 의도된 동작, gitleaks 와 같음).

```zsh
python scripts/pin_terraform.py
```

```zsh
python -m pytest -q
```

실패 0 이어야 합니다. 그다음 `git status -sb` 에 `.env`·zip·`data/`·`*.tfstate`·`*.tfvars` 가 없는지 보고 commit·push 한 뒤 Actions 의 `ci` 가 초록인지 확인합니다(새 `terraform` 잡 포함).

## 1. 예산 알림 (가장 먼저, 포털)

비용 관리 + 청구 → 비용 관리 → 예산 → 추가: 범위 = 구독(Azure for Students), 월별, ₩50,000, 경보 = 실제 비용 50%·80%, 수신자 = 내 이메일. (Olist 월 약 ₩17K 가 이미 포함됩니다.)

## 2. 로그인 · 키 · 내 IP

```zsh
az login
```

```zsh
az account show --query name -o tsv
```

기대: `Azure for Students`. 구독 ID 는 **환경 변수로만** 넘깁니다(출력하지 않음).

```zsh
export ARM_SUBSCRIPTION_ID="$(az account show --query id -o tsv)"
```

SSH 키가 없으면 만듭니다(있으면 건너뜀).

```zsh
ls ~/.ssh/urbanstream_azure.pub || ssh-keygen -t ed25519 -f ~/.ssh/urbanstream_azure -C urbanstream-azure
```

내 공인 IP 를 **터미널에서만** 확인하고 tfvars 에 넣습니다(채팅에 붙이지 않음).

```zsh
curl -s https://api.ipify.org; echo
```

```zsh
cp infra/terraform/terraform.tfvars.example infra/terraform/terraform.tfvars
```

`infra/terraform/terraform.tfvars` 를 편집기로 열어 `allowed_ssh_cidr = "내IP/32"` 로 바꿉니다. 이 파일은 git 에서 제외됩니다.

## 3. 만들기 (terraform)

```zsh
cd infra/terraform
```

```zsh
terraform init
```

`.terraform.lock.hcl` 이 생기면 **커밋**합니다(provider 버전 고정). `.terraform/` 는 커밋하지 않습니다.

```zsh
terraform plan -no-color | tee /tmp/plan.txt
```

기대: 리소스 약 8개 생성(그룹·vnet·subnet·NSG·공용 IP·NIC·연결·VM), 변경·삭제 0. `Standard_B2s_v2` 가 막히면 오류 문구를 알려 주세요(D-225/228 은 목록·쿼터만 확인했고 실제 생성은 처음).

```zsh
terraform apply
```

`yes` 를 입력합니다. 끝나면 `cd ../..` 로 프로젝트 루트로 돌아옵니다. 이 시점부터 **비용이 쌓입니다**(시작 시각을 `08_summary.md` 에 적어 두세요).

## 3-B. (MFA 오류 우회) Cloud Shell 에서 apply — 3절 `apply` 가 `RequestDisallowedByAzure ... MFA` 로 거부될 때만

증상(11-12 실제 발생): `terraform apply` 가 첫 리소스(리소스 그룹)에서 401 `RequestDisallowedByAzure: ... without authenticating through MFA`. 확인: `az account get-access-token` 토큰의 `amr` 가 `['pwd']` 뿐이고(`mfa` 없음), 같은 계정이 **포털에서는** 리소스 그룹을 바로 만든다 — 포털 세션 토큰에만 MFA 가 있다. 그래서 인증을 포털 세션에서 받는 **Cloud Shell** 로 `apply` 만 한다(나머지는 Mac). 이 경우 state 는 Cloud Shell 에 있고 Mac 에는 없으며, 지울 때는 포털에서 리소스 그룹을 삭제한다.

1. Mac 에서 업로드용 묶음을 만든다(비밀 없음 — 공개키와 내 IP 가 든 tfvars 만 들어간다. 이 파일을 채팅·git 에 올리지 않는다).

```zsh
cd infra/terraform && zip -j ~/Desktop/tf_upload.zip main.tf variables.tf outputs.tf versions.tf cloud-init.yaml.tftpl terraform.tfvars .terraform.lock.hcl ~/.ssh/urbanstream_azure.pub && cd ../..
```

2. 포털 상단의 Cloud Shell 아이콘 → **Bash**. 스토리지 설정 화면이 나오면 "스토리지 계정 필요 없음"(임시 세션)을 고른다 — state 가 필요 없고(삭제는 포털) 다른 리소스를 만들지 않는다. 구독은 `Azure for Students`.
3. Cloud Shell 의 업로드 버튼으로 `tf_upload.zip` 을 올린 뒤:

```bash
mkdir -p ~/tf && cd ~/tf && unzip -o ~/tf_upload.zip && mkdir -p ~/.ssh && mv urbanstream_azure.pub ~/.ssh/
```

```bash
export ARM_SUBSCRIPTION_ID="$(az account show --query id -o tsv)"
```

```bash
terraform init
```

```bash
terraform apply
```

`yes` 입력. 끝나면 공용 IP 를 확인한다(이 IP 를 Mac 터미널에 옮겨 적는다).

```bash
terraform output -raw public_ip
```

4. Mac 터미널(프로젝트 루트)에서 아래 세 변수를 설정한다. 이후 `scripts/cloud.sh dump / up / tunnel / ps` 는 그대로 쓴다(5~6절).

```zsh
export CLOUD_HOST="<위에서 확인한 공용 IP>" CLOUD_USER=azureuser CLOUD_RG=urbanstream-step12-rg
```

7절의 `scripts/cloud.sh destroy` 는 `CLOUD_RG` 가 있으면 포털에서 리소스 그룹을 지우라고 안내하고, 지운 뒤 `az group exists` 로 확인한다. (Cloud Shell 임시 세션은 유휴 20분 뒤 사라질 수 있다 — `apply` 가 끝난 뒤에는 필요 없다.) 세션이 `apply` 도중 끊기면 포털에서 `urbanstream-step12-rg` 를 지우고 처음부터 다시 한다.

## 4. Mac 에서 스냅샷 만들기

Mac 스택이 켜져 있어야 합니다(`docs/runbooks/mac-on-off.md` 2번; 덮개 닫지 않기). **마트가 최신인지**(대시보드 건강 화면의 Gold 마트 최신 시각, Airflow 의 `gold_intraday` 초록) 먼저 봅니다 — 스냅샷에는 그 시점까지만 들어갑니다.

```zsh
scripts/cloud.sh dump
```

기대: `data/cloud/snapshot_<시각>.sql.gz` 와 크기 출력. 크기가 너무 크면(수백 MB 이상) 알려 주세요 — 기간 제한을 추가합니다. 이 파일은 `data/` 아래라 git 에 올라가지 않습니다.

## 5. VM 에 올려 기동

```zsh
scripts/cloud.sh up
```

하는 일: VM 준비(cloud-init) 대기 → 소스 전송(`.gitignore` 에 걸린 `.env`·`data/`·state·tfvars 제외) → 스냅샷 전송 → VM 에서 `.env` 새 비밀 생성 → postgres → **마이그레이션 001~014 를 빈 DB 에 적용(D-202, 처음 실행)** → `api_service` 로그인 → 스냅샷 복원 → api·dashboard 빌드·기동 → `/health`·`/ready`·`pipeline overall` 출력.
기대(끝부분): `/health` ok, `/ready` ok, `pipeline overall: …`. 첫 실행은 이미지 빌드로 수 분 걸립니다. 복원은 한 트랜잭션이라 실패하면 반쪽 데이터가 남지 않습니다.

```zsh
scripts/cloud.sh ps
```

기대: postgres · api · dashboard 가 `running (healthy)`.

## 6. 접속하고 캡처 (SSH 터널)

별도 터미널 탭에서(끝낼 때 Ctrl+C):

```zsh
scripts/cloud.sh tunnel
```

브라우저: API 문서 `http://127.0.0.1:18010/docs`, 대시보드 `http://127.0.0.1:18501`. (Mac 로컬 스택의 8010·8501 과 겹치지 않게 18010·18501 입니다.)
- 캡처 대상: `docs/evidence/step12/README.md` 표. **주소창에 구독 ID·IP 가 보이면 가립니다.**
- 대시보드의 "오래됨(stale)" 표시: 스냅샷이라 곧 나옵니다(기본 3시간). 정상입니다 — 캡처에 "스냅샷, 촬영 시각 X" 를 같이 적습니다. 보기 싫으면 VM 의 `.env` 에 `API_STALE_AFTER_SEC` 를 크게 넣고 api 를 재기동할 수 있지만 증거 문서에는 스냅샷임을 밝힙니다.
- 대시보드 메모리·CPU 확인: `scripts/cloud.sh ssh docker stats --no-stream` (B2s_v2 로 충분한지 판단 근거).
- 터널로 Streamlit 이 안 열리면(웹소켓) 알려 주세요 [미확인].

## 7. 비용 기록 + 지우기

포털 Cost Management 에서 비용 그래프를 캡처합니다(반영에 몇 시간~하루 지연될 수 있음). 그다음 지웁니다.

```zsh
scripts/cloud.sh destroy
```

`yes` 입력 후 `확인: 리소스 그룹 …가 사라졌습니다.` 가 나와야 합니다. 안 나오면 남은 리소스가 표로 나옵니다. 하루쯤 뒤 비용을 한 번 더 캡처해 destroy 뒤 비용이 멈췄는지 봅니다.

## 문제 해결

| 증상 | 원인 [추정] | 조치 |
|---|---|---|
| `scripts/cloud.sh up` 이 "VM 준비를 확인하지 못했습니다" | 내 공인 IP 가 바뀌어 NSG 가 막음 / cloud-init 실패 | tfvars 의 `allowed_ssh_cidr` 수정 → `terraform apply`. cloud-init 은 `scripts/cloud.sh ssh sudo cat /var/log/cloud-init-output.log` |
| `terraform apply` 가 MFA 오류(401 `RequestDisallowedByAzure`) | CLI 토큰에 MFA 없음(`amr` = `['pwd']`) | 3-B 절(Cloud Shell) |
| `terraform apply` 가 크기/쿼터 오류 | 리전 vCPU 한도 6, 용량 | 오류 문구를 알려 주세요. `vm_size` 변경 |
| 마이그레이션 단계에서 실패 | 첫 클라우드 실행(D-202) | 출력의 파일명·오류를 알려 주세요(비밀 제외). 이미 적용된 SQL 은 수정하지 않습니다 |
| 복원 단계에서 오류 | pg_dump↔psql 버전·FK | 한 트랜잭션이라 데이터는 비어 있음. 오류 문구를 알려 주세요 |
| 대시보드 화면이 비어 있음/401 | 키 | VM 의 `.env` 는 `init-env` 가 만든 키를 대시보드·API 가 공유. `scripts/cloud.sh ssh "cd urbanstream && docker compose -f docker-compose.yml -f docker-compose.cloud.yml --profile core --profile api --profile dashboard logs --tail=50 api"` |
| `docker compose ... --wait` 오류 | VM 의 Compose 버전이 Mac(v2.10.2)과 다름(D-209) | 오류 문구를 알려 주세요 |
| 터널 "address already in use" | 이전 터널이 살아 있음 | 이전 탭에서 Ctrl+C |

## 끝낸 뒤
`HANDOFF.md`·`docs/DEFERRED.md` L장을 결과로 갱신합니다(새 채팅에 apply/destroy 출력과 비용을 알려 주세요 — 비밀 가리고).
