# Terraform modules

Reusable AWS VPC and application load-balancer modules. These are infrastructure examples; validation does not provision resources or verify a deployed environment.

[Portfolio](../../README.md) · [Verification](../../docs/VERIFICATION.md)

| Module | Purpose |
| --- | --- |
| [VPC](vpc/README.md) | Public/private subnets, explicit route-table associations and optional NAT egress. Private subnets retain their private route table when NAT is disabled. |
| [Application Load Balancer](alb/README.md) | Security groups, target groups, HTTP forwarding or HTTPS redirection, and a TLS listener. |

Use Terraform 1.9.8, matching CI. In each module directory:

```sh
terraform fmt -check
terraform init -backend=false
terraform validate
terraform test
```

The tests use a mocked AWS provider and do not create cloud resources. Provider downloads require network access. Lockfiles record provider selections; the root workflow validates the modules from a fresh checkout.

For the ALB's default HTTPS/redirect configuration, supply a valid ACM certificate. Plain HTTP test environments must explicitly select `create_https_listener=false` and `http_listener_action_type="forward"`. Created security groups are attached automatically; disabling creation requires existing group IDs. A forwarding listener requires a target group.

Before any real deployment, choose a region and account, review a plan and estimated costs, and arrange remote state, least-privilege credentials, access logs and cleanup. NAT gateways and load balancers can incur ongoing charges.
