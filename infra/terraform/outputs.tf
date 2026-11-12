output "public_ip" {
  description = "SSH 접속용 공인 IP (scripts/cloud.sh 가 읽는다)"
  value       = azurerm_public_ip.this.ip_address
}

output "admin_username" {
  value = var.admin_username
}

output "resource_group" {
  description = "destroy 뒤 이 그룹이 사라졌는지 확인한다 (scripts/cloud.sh destroy)"
  value       = azurerm_resource_group.this.name
}
