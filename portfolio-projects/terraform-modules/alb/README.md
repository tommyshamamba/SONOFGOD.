# Application Load Balancer Module

Reusable Terraform module for creating AWS Application Load Balancers with target groups and listeners.

## Usage

```hcl
module "alb" {
  source = "../../terraform-modules/alb"

  name               = "my-app-alb"
  internal           = false
  vpc_id             = module.vpc.vpc_id
  subnet_ids         = module.vpc.public_subnet_ids
  acm_certificate_arn = var.acm_certificate_arn

  create_target_group = true
  target_group_port    = 80
  target_group_protocol = "HTTP"

  tags = {
    Environment = "dev"
  }
}
```

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|----------|
| name | Name of the load balancer | string | - | yes |
| internal | Whether the load balancer is internal | bool | false | no |
| load_balancer_type | Type of load balancer | string | application | no |
| vpc_id | VPC ID | string | - | yes |
| subnet_ids | List of subnet IDs | list(string) | - | yes |
| security_group_ids | List of security group IDs | list(string) | [] | no |
| create_security_group | Create security group | bool | true | no |
| create_target_group | Create target group | bool | true | no |
| target_group_port | Target group port | number | 80 | no |
| target_group_protocol | Target group protocol | string | HTTP | no |
| target_type | Target type | string | ip | no |
| health_check_enabled | Enable health checks | bool | true | no |
| health_check_path | Health check path | string | / | no |
| create_http_listener | Create HTTP listener | bool | true | no |
| http_listener_action_type | HTTP listener action | string | redirect | no |
| create_https_listener | Create HTTPS listener | bool | true | no |
| acm_certificate_arn | ACM certificate ARN | string | "" | no |
| enable_deletion_protection | Enable deletion protection | bool | false | no |
| tags | Additional tags | map(string) | {} | no |

## Outputs

| Name | Description |
|------|-------------|
| lb_id | Load balancer ID |
| lb_arn | Load balancer ARN |
| lb_dns_name | Load balancer DNS name |
| lb_zone_id | Load balancer zone ID |
| target_group_id | Target group ID |
| target_group_arn | Target group ARN |
| security_group_id | Security group ID |
| http_listener_arn | HTTP listener ARN |
| https_listener_arn | HTTPS listener ARN |
