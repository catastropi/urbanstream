# Step 12 증거 묶음 — Azure 서빙 계층 배포 → 파괴 → 재배포 → 파괴

**한 줄 요약.** 서빙 계층(PostgreSQL·FastAPI·Streamlit 대시보드)을 Azure 의 VM 1대에 올려 5일간 가동했고, 같은 Terraform 코드로 파괴·재생성·재서비스까지 확인한 뒤 모두 삭제했다. 스트리밍·배치(Kafka·Spark·Airflow·수집기)는 올리지 않았다 — 로컬(Mac)에서만 돈다.

> 이 폴더의 모든 이미지는 **원본이 아니다.** 계정 이름(3곳)·허용 SSH IP·VM 공인 IP 는 검은 박스로 가렸다(원본은 저장소에 없다). 구독 ID·이메일은 애초에 화면에 없는 것만 골랐다.

## 사실 (근거는 아래 이미지)

| 항목 | 값 | 근거 |
|---|---|---|
| 리전·리소스 그룹 | Korea Central, `urbanstream-step12-rg` | 04 |
| 리소스 | 8개 (RG, VNet, 서브넷, NSG, NSG 연결, 공용 IP, NIC, VM) — 포털 목록은 디스크 포함 6개 유형 | 01·02·04 |
| VM 크기 | `Standard_B2s_v2` (2 vCPU / 8 GiB) | 코드(`infra/terraform`), 포털 화면에는 크기 미표시 |
| 가동 | 컨테이너 `api`·`dashboard` 26시간·`postgres` 4일(모두 healthy) 시점의 캡처 | 06 |
| 접속 방식 | 공개 안 함. SSH 터널(`scripts/cloud.sh tunnel`)로 `127.0.0.1` 에서만 | 05·07·09 |
| 데이터 | Mac 의 Gold/마트 `pg_dump` 스냅샷(**2026-11-12 15:27 KST**) 복원. 이후 갱신 없음 | 11~17 |
| 비용(RG 필터, 11-12~11-17) | **₩17,587.77** — VM ₩15,724.70 · Storage ₩1,100.32 · Virtual Network ₩762.73 · Bandwidth ₩0.01. 일 약 ₩3.5K | 10 |
| 파괴 | `terraform destroy` → `Destroy complete! Resources: 8 destroyed.`, `az group exists` → `false` | 18·19 |

## 이미지 목록과 설명

### A. 코드로 만들고 코드로 지웠다 (IaC)
| 파일 | 보여 주는 것 |
|---|---|
| `01_iac_plan_8_to_add.png` | 재배포 전 `terraform plan`: 8개 추가, 출력값(admin_username·public_ip·resource_group) |
| `02_iac_apply_8_added.png` | `terraform apply`: `Apply complete! Resources: 8 added` (공인 IP 가림) |
| `03_iac_plan_no_changes.png` | apply 직후 `terraform plan`: `No changes. Your infrastructure matches the configuration.` |
| `18_iac_destroy_8_destroyed.png` | `terraform destroy`: 8개 파괴 |
| `19_rg_deleted_exists_false.png` | `az group exists --name urbanstream-step12-rg` → `false` (삭제 확인) |

### B. 배포된 모습 (첫 번째 VM, 5일 가동)
| 파일 | 보여 주는 것 |
|---|---|
| `04_rg_resources_6_listed.png` | 리소스 그룹의 공용 IP·NIC·NSG·VM·OS 디스크·VNet |
| `05_nsg_ssh_rule.png` | NSG 규칙 `ssh-from-my-ip`(22/TCP, 허용). 원본 주소는 가렸다 — 내 IP 한 곳에서만 SSH 허용 |
| `06_vm_containers_healthy.png` | VM 안 `docker compose ps`: api·dashboard·postgres 전부 `healthy` |
| `10_cost_analysis.png` | Cost Management: RG 필터, 일별 비용, 서비스별 비용(VM 이 약 89%) |

### C. API (`127.0.0.1:18010`, 터널 경유)
| 파일 | 보여 주는 것 |
|---|---|
| `07_api_swagger_docs.png` | Swagger UI: `/health`·`/ready`·`/v1/pipeline/*` |
| `08_api_health_200.png` | `/health` → 200 `{"status":"ok","version":"1.0.0"}` |
| `09_api_ready_200_via_tunnel.png` | `/ready` → 200 `{"status":"ok","db":"ok"}`, 주소가 터널(127.0.0.1) 임을 보여 줌 |

**참고 — `/v1/...` 를 Swagger 에서 그냥 실행하면 401 이다.** 이는 설계다: `/health`·`/ready`·`/docs` 를 뺀 모든 경로는 `X-API-Key` 헤더가 필요하다(401 `missing_api_key`). 위 증거는 인증이 필요 없는 두 경로로 "API 가동과 DB 연결"을 보였고, 인증이 필요한 `/v1` 호출 결과는 대시보드 화면(D)이 보여 준다(대시보드가 전용 키로 API 를 호출한다). 키는 화면에 보이지 않게 했다.

### D. 대시보드 7개 화면 (`127.0.0.1:8501`, 터널 경유)
| 파일 | 화면 |
|---|---|
| `11_dash_overview.png` | 개요 — 구성요소 21개(정상 15·저하 1·중단 5), 소스별 신선도 |
| `12_dash_map.png` | 지도 — 서울 지도, 돌발 위치, 링크 4개 |
| `13_dash_congestion.png` | 혼잡(A) — 링크별 속도·단계 |
| `14_dash_incident_impact.png` | 돌발 영향(B) — 진행 중 68건, 유형별 영향 |
| `15_dash_bus.png` | 버스(C) — 도착 예측 변동 |
| `16_dash_weather.png` | 날씨(D) — 강수 구분별 속도 비율 |
| `17_dash_anomaly_health.png` | 이상·건강(E·F) — 파이프라인 구성요소 상태 |

**왜 화면에 "오래됨"·"중단"이 보이나.** 이 VM 은 수집·처리를 하지 않고 **11-12 15:27 KST 에 뜬 스냅샷**만 보여 준다. 대시보드가 "경과 3일 21시간, 적재 11-12 15:27 KST"라고 스스로 밝히는 것은 오류가 아니라 정직한 표시 규칙(Step 10)이 동작한 것이다. 소스 5개 중 '중단' 표시도 같은 이유다. 캡처 시각은 화면 왼쪽의 "지금 11-16 13:0x KST".

## 이 증거가 말해 주는 것·주지 않는 것
- **말해 주는 것:** 서빙 계층 3개 컨테이너를 Azure VM 에서 코드 한 벌(Terraform + `scripts/cloud.sh` + `docker-compose.cloud.yml`)로 재현할 수 있다. 스냅샷을 복원해 API·대시보드가 실제 값을 보여 준다. SSH 터널로만 접속한다. 비용을 측정했고 끝나면 지운다.
- **주지 않는 것:** Azure 에서 실시간 수집·Airflow·Spark 를 돌렸다는 증거가 아니다(올리지 않았다). 24시간 무중단 운영 증거가 아니다. 학생 구독 크레딧과 vCPU 한도(리전 6) 때문에 범위를 일부러 줄였다(DEFERRED D-224·D-228).

## 부활(재배포) 검증 — 이미지 없음
11-17 오후에 `terraform apply`(Cloud Shell) → `scripts/cloud.sh up`(소스·스냅샷 전송, 스택 기동) → `scripts/cloud.sh tunnel` 까지 다시 해서 API·대시보드가 정상 동작함을 사용자가 확인했다. **이 확인에는 캡처가 없다** — 사용자 보고만 있다. 절차는 `docs/runbooks/step12-azure.md`.

## 남은 확인 (사용자)
- 포털 비용 분석에서 destroy 이후 일별 비용이 0 으로 떨어졌는지(11-19경, 두 번째 VM 의 비용 포함) — DEFERRED D-254.
- 포털에서 리소스 그룹이 없다는 화면 캡처(선택): 지금은 터미널 `false` 만 있다.
