# Step 12 (ADR-0014): Azure 최소 증거 배포 — VM 1대 + 네트워크. 서빙 계층(postgres·api·dashboard)만 올린다.
# 인증은 `az login` 의 로그인 세션을 쓴다. 구독 ID 는 코드·tfvars 에 쓰지 않고 환경 변수로 준다:
#   export ARM_SUBSCRIPTION_ID="$(az account show --query id -o tsv)"
terraform {
  required_version = ">= 1.9.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }
}

provider "azurerm" {
  features {}
}
