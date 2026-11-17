# Step 12 런북 — Azure 서빙 계층 만들기·올리기·지우기 (시연용 부활 절차)

> **`step12-cloud.md` 와의 관계:** `step12-cloud.md` 는 11-12 코드 완료 시점의 절차서(내 IP 확인·예산 알림·dump·up·터널·destroy 전체)이고, 이 문서는 11-17 실제 실행에서 확인된 Cloud Shell 경로·부활 절차·막혔을 때 표를 담는다. 순서는 `step12-cloud.md`, 막히면 이 문서.

**사용자가 직접 실행합니다.** 샌드박스는 Azure 에 접근할 수 없습니다. 비밀(API 키·`.env`·`terraform.tfvars`·tfstate)·구독 ID·IP 는 채팅·git 어디에도 붙이지 않습니다. 근거: DEFERRED P장 D-250~D-254, 증거 `docs/evidence/step12/`.

## 0. 이 절차가 하는 일과 안 하는 일
- **하는 일:** VM 1대(Korea Central, `Standard_B2s_v2`) + 네트워크·NSG(SSH 는 내 IP 만) 를 `terraform` 으로 만들고, `scripts/cloud.sh up` 이 소스·마트 스냅샷을 올려 `postgres`·`api`·`dashboard` 를 띄운다. 접속은 SSH 터널만(공개 안 함).
- **안 하는 일:** Kafka·Spark·Airflow·monitor·수집기는 올리지 않는다. 그래서 VM 에 외부 API 키가 없고, 대시보드는 **스냅샷을 찍은 시각의 값**을 "오래됨"으로 표시한다(정상).
- 켜 두면 하루 약 ₩3.5K(11-12~11-17 실측, 5일 합계 ₩17,587.77). **시연이 끝나면 바로 지운다.**

## 1. 전체 순서 한눈에
1. (Mac) 스냅샷 파일과 내 공인 IP 확인 → 2. (Cloud Shell) 파일 올리기·`terraform apply` → 3. (Mac) `cloud.sh up` → 4. (Mac) `cloud.sh tunnel` → 시연 → 5. (Cloud Shell) `terraform destroy` → `az group exists` = `false` → 6. 비용 확인.
`terraform` 은 **Cloud Shell** 에서 돌린다: Mac 의 `az` 는 학교 계정 MFA 때문에 `AADSTS50076` 으로 막힌다(D-252). Cloud Shell 은 포털에서 이미 로그인된 상태라 문제가 없다.

## 2. 준비 (Mac)

1. **스냅샷 파일**이 있어야 `up` 이 돈다. `data/` 는 git 에 없으니 지우지 않는다.

```zsh
ls -1t data/cloud/snapshot_*.sql.gz | head -1
```

   더 최근 데이터로 시연하고 싶으면 Mac 스택을 켠 뒤(`mac-on-off.md`) 새 스냅샷을 만든다.

```zsh
scripts/cloud.sh dump
```

2. **내 현재 공인 IP** 를 확인한다(채팅에 붙이지 않는다).

```zsh
curl -s https://ifconfig.me
```

3. `infra/terraform/terraform.tfvars` 의 `allowed_ssh_cidr` 가 위 IP 의 `/32` 인지 본다. **장소·네트워크가 바뀌면 달라진다.** 다르면 이 파일을 고친다(git 에 올리지 않는 파일).

```zsh
grep allowed_ssh_cidr infra/terraform/terraform.tfvars
```

## 3. 인프라 만들기 (Azure 포털 Cloud Shell)

1. 포털(`portal.azure.com`) 오른쪽 위 `>_` → **Bash**. 프롬프트가 `이름@Azure:~$` 모양이면 맞다(Mac 터미널이 아니다).
2. `Manage files` → `Upload` 로 아래 파일을 올린다.
   - `infra/terraform/` 의 `main.tf`, `variables.tf`, `outputs.tf`, `versions.tf`, `cloud-init.yaml.tftpl`, `terraform.tfvars`
   - SSH **공개키** `~/.ssh/urbanstream_azure.pub` (이름이 `.pub` 로 끝나는 것만. **개인키 `urbanstream_azure` 는 절대 올리지 않는다.**)
   - **올리지 않는 것:** `terraform.tfstate`, `terraform.tfstate.backup`(Mac 의 것은 비어 있다/오래됐다)
3. 올린 공개키를 제 위치로 옮긴다(`main.tf` 가 `~/.ssh/urbanstream_azure.pub` 를 읽는다).

```
mkdir -p ~/.ssh
```

```
mv ~/urbanstream_azure.pub ~/.ssh/urbanstream_azure.pub
```

4. 초기화 → 계획 → 적용.

```
terraform init
```

```
terraform plan
```

   **빈 상태에서 `8 to add`** 가 나와야 정상이다. 이때 같은 이름의 리소스 그룹이 **이미 Azure 에 있으면 apply 하지 않는다**(state 와 현실이 달라 충돌한다). 먼저 `az group exists --name urbanstream-step12-rg` 가 `false` 인지 본다.

```
terraform apply
```

   중간에 `yes`. 마지막에 `Apply complete! Resources: 8 added` 와 `public_ip` 출력이 나온다. **캡처할 때 `public_ip` 줄과 `plan` 의 `source_address_prefix` 줄은 불투명한 검은 박스로 가린다.**

5. 공인 IP 는 Cloud Shell 에서 다시 볼 수 있다(채팅에 붙이지 않는다).

```
terraform output public_ip
```

## 4. 서비스 올리기 (Mac)

1. 접속 정보를 환경 변수로 준다. Mac 에는 이번 배포의 state 가 없으므로 이 방식이 필요하다(`cloud.sh` 주석의 "3-B" 경로).

```zsh
export CLOUD_HOST=<공인 IP> CLOUD_USER=azureuser CLOUD_RG=urbanstream-step12-rg
```

2. 올리기. VM 의 cloud-init(Docker 설치)이 끝나길 최대 약 5분 기다린 뒤, 소스(`.gitignore` 에 걸린 `.env`·`data/`·state·tfvars 제외)·스냅샷을 보내고 VM 에서 이미지를 빌드해 기동한다. 첫 실행은 수 분 걸린다.

```zsh
scripts/cloud.sh up
```

3. 상태: `postgres`·`api`·`dashboard` 가 `healthy` 여야 한다.

```zsh
scripts/cloud.sh ps
```

4. 터널(끝낼 때 Ctrl+C). 터미널에 안내된 주소를 브라우저로 연다. API 는 `http://127.0.0.1:18010/docs`·`/ready`, 대시보드는 안내된 로컬 포트.

```zsh
scripts/cloud.sh tunnel
```

## 5. 끝내기 — 비용이 나가니 바로
1. 터널 터미널에서 Ctrl+C.
2. **Cloud Shell 에서** 파괴(`yes` 입력). `Destroy complete! Resources: 8 destroyed.` 확인·캡처.

```
terraform destroy
```

3. 삭제 확인(`false` 가 나와야 한다).

```
az group exists --name urbanstream-step12-rg
```

4. Mac 터미널의 환경 변수를 지운다(옛 IP 를 가리키지 않게).

```zsh
unset CLOUD_HOST CLOUD_USER CLOUD_RG
```

5. 이틀쯤 뒤 포털 비용 분석(범위: 내 계정, 필터: 리소스 그룹 `urbanstream-step12-rg`)에서 일별 비용이 0 인지 본다.

> **`scripts/cloud.sh destroy` 는 쓰지 않는다.** `CLOUD_RG` 가 설정돼 있으면 "포털에서 리소스 그룹을 지우세요"라고 안내하고 Enter 를 기다릴 뿐 직접 지우지 않는다. 위 2번의 `terraform destroy` 가 정식이다. (Cloud Shell 에 state 가 없으면 포털에서 리소스 그룹을 삭제하거나 Cloud Shell 의 `az group delete --name urbanstream-step12-rg --yes` 로 지운다.)

## 6. 막혔을 때

| 증상 | 원인 | 조치 |
|---|---|---|
| Mac 에서 `az`·`terraform` 이 `AADSTS50076`(MFA 필요) | 학교 계정 MFA | Cloud Shell 에서 실행(3·5절). Mac 에서 `az login` 을 반복하지 않는다 |
| Mac 에서 `terraform plan` 이 리소스가 있는데도 `8 to add` | Mac 의 `terraform.tfstate` 는 비어 있다(`resources: []`) — 배포가 Mac 에서 이루어지지 않은 것으로 보인다 [추정] | **apply 하지 않는다**(충돌·중복). Cloud Shell 에서 하거나, 기존 리소스 그룹을 지우고 다시 만든다(D-251) |
| `terraform plan`: `no file exists at ...urbanstream_azure.pub` | 공개키가 Cloud Shell 에 없다 | 3절 2~3번 |
| `terraform init`: `empty directory` | `.tf` 파일을 올리지 않았다 | 3절 2번 파일 목록 |
| `cloud.sh up`: `ssh: connect ... Operation timed out` 이 반복 | **NSG 가 내 현재 IP 를 막고 있다**(허용 IP 는 tfvars 에 적은 하나뿐). 이동했거나 네트워크가 바뀌면 생긴다 | 2절 2·3번으로 IP 비교 → Cloud Shell 의 `terraform.tfvars` 의 `allowed_ssh_cidr` 를 현재 IP/32 로 고치고 `terraform apply`(NSG 규칙 1개만 바뀜) → `up` 재실행. 이번 부활 때 이 조치 뒤 `up`·`tunnel` 이 통과했다(허용 IP 불일치가 원인이라는 확정 기록은 없음 [추정]) |
| `cloud.sh up`: "전송 목록에 비밀/상태 파일이 섞여 있어 중단" | `.gitignore` 에 안 걸린 `.env*`·`*.tfstate`·`*.tfvars`·`data/` 가 있다 | `.gitignore` 를 고친다. 우회하지 않는다 |
| `cloud.sh up`: "스냅샷이 없습니다" | `data/cloud/snapshot_*.sql.gz` 없음 | `scripts/cloud.sh dump`(Mac 스택이 떠 있어야 한다) |
| `/v1/...` 가 Swagger 에서 401 | 설계(`X-API-Key` 필요). `/health`·`/ready`·`/docs` 만 인증 없음 | Swagger `Authorize` 에 `.env` 의 키를 입력. 키·Curl 박스는 캡처하지 않는다 |
| 대시보드에 "오래됨"·"중단" | 수집을 안 올렸다. 스냅샷 시각의 값이다 | 정상. 증거 문서 참고 |

## 7. 알려진 한계·주의
- **state 가 어디에도 보관되지 않는다.** 이번 배포의 첫 apply 경로와 state 위치를 확인하지 못했고(Mac 은 비어 있고 Cloud Shell 홈에도 없었다), 지울 때는 `az group delete` 로 처리했다(D-251). 다음부터는 apply 직후 Cloud Shell 에서 `terraform.tfstate` 를 받아 두거나 원격 backend 를 쓰는 것을 검토한다(선택, D-251).
- 부활에 걸린 시간은 재지 않았다 [미측정]. VM 생성·cloud-init·이미지 빌드를 합쳐 수십 분 단위로 보면 된다 [추정].
- VM 에는 외부 API 키가 없다. 수집을 하려면 키를 VM 에 넣어야 하고, 그 전에 `.env.bak` 키 문제(D-195)를 다시 판단해야 한다.
- 크레딧: 학생 구독 ₩133,765, 2027-08-15 만료(D-229). 시연 1일 ≈ ₩3.5K 이므로 예산 안이지만, 지우는 것을 잊지 않는다.
