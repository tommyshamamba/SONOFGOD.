output "lb_id" {
  description = "ID of the load balancer"
  value       = aws_lb.this.id
}

output "lb_arn" {
  description = "ARN of the load balancer"
  value       = aws_lb.this.arn
}

output "lb_dns_name" {
  description = "DNS name of the load balancer"
  value       = aws_lb.this.dns_name
}

output "lb_zone_id" {
  description = "Zone ID of the load balancer"
  value       = aws_lb.this.zone_id
}

output "target_group_id" {
  description = "ID of the target group"
  value       = var.create_target_group ? aws_lb_target_group.this[0].id : null
}

output "target_group_arn" {
  description = "ARN of the target group"
  value       = var.create_target_group ? aws_lb_target_group.this[0].arn : null
}

output "security_group_id" {
  description = "ID of the security group"
  value       = var.create_security_group ? aws_security_group.alb[0].id : null
}

output "http_listener_arn" {
  description = "ARN of the HTTP listener"
  value       = var.create_http_listener ? aws_lb_listener.http[0].arn : null
}

output "https_listener_arn" {
  description = "ARN of the HTTPS listener"
  value       = var.create_https_listener && var.acm_certificate_arn != "" ? aws_lb_listener.https[0].arn : null
}
