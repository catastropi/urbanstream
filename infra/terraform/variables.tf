variable "allowed_ssh_cidr" {
  description = "SSH(22)를 허용할 내 공인 IP 의 CIDR (예: 203.0.113.7/32). 0.0.0.0/0 은 거부한다."
  type        = string

  validation {
    condition     = can(cidrnetmask(var.allowed_ssh_cidr)) && !contains(["0.0.0.0/0", "::/0"], var.allowed_ssh_cidr) && tonumber(split("/", var.allowed_ssh_cidr)[1]) >= 24
    error_message = "allowed_ssh_cidr 는 /24 이상으로 좁은 IPv4 CIDR 이어야 하고 0.0.0.0/0 은 안 된다. 내 IP 한 개면 x.x.x.x/32."
  }
}

variable "ssh_public_key_path" {
  description = "VM 에 넣을 SSH 공개키 파일 경로(.pub). 개인키는 절대 넣지 않는다."
  type        = string
  default     = "~/.ssh/urbanstream_azure.pub"
}

variable "location" {
  description = "리전. Korea Central 에 Standard_B*_v2 가 있고 이 구독에 제한이 없음을 확인했다(D-225)."
  type        = string
  default     = "koreacentral"
}

variable "vm_size" {
  description = "VM 크기. 서빙 계층 실측 합계가 약 0.4 GiB 라 2 vCPU/8 GiB 로 시작한다. 모자라면 Standard_B4s_v2 (리전 vCPU 한도 6 이내, D-228)."
  type        = string
  default     = "Standard_B2s_v2"
}

variable "admin_username" {
  description = "VM 관리자 계정 이름 (비밀번호 로그인은 끈다)"
  type        = string
  default     = "azureuser"
}

variable "name_prefix" {
  description = "리소스 이름 접두사. 리소스 그룹은 <접두사>-rg 이고, 이 그룹 안의 것만 만들고 지운다(Olist 리소스는 건드리지 않는다)."
  type        = string
  default     = "urbanstream-step12"
}

variable "os_disk_gb" {
  description = "OS 디스크 크기(GB). 도커 이미지 빌드와 DB 스냅샷 복원에 쓴다."
  type        = number
  default     = 64
}
