# Blockchain infrastructure example

This directory describes an AWS VPC, EKS cluster, Redis, ALB, logs, S3 assets and optional RDS. It is separate from the local Docker/Kubernetes demonstration. Schema validation does not establish that these resources have been provisioned or that the application is connected to them.

## Validate without an AWS deployment

From this directory, with Terraform installed:

```sh
terraform init -backend=false -input=false
terraform fmt -check
terraform validate
```

Initialization downloads the pinned modules/providers. `-backend=false` prevents access to the configured remote state. Validation does not need AWS credentials, provision resources or make a Terraform plan. Commit `.terraform.lock.hcl`; keep `.terraform/` and state files out of Git.

## Before planning a sandbox deployment

- Configure the existing S3 backend block in `main.tf` for your own state bucket and lock table. Do not add a second backend/provider block. Create those state resources separately if needed.
- Copy `terraform.tfvars.example` to a private variables file. Supply an EKS version currently supported in the selected region and a valid ACM certificate ARN in that region. Neither has an unsafe placeholder default. The listener requires HTTPS; an empty certificate is not an HTTP-only configuration.
- Review module/provider compatibility with the selected EKS version. The module versions are pinned for reproducible validation; AWS provisioning against a chosen version remains untested.
- RDS is disabled by default because the application does not use it. If enabling the database example, select an available PostgreSQL engine/instance version and provide `TF_VAR_database_password` through a secret environment variable. Do not place a real password in the example file or commit a private variables file. State can contain secrets and requires restricted access.
- Review the plan in an isolated AWS account before any apply. NAT gateways, EKS, databases and other resources may incur charges. This repository has not applied this configuration.

## Application wiring still required for cloud use

This example is not an end-to-end deployment pipeline. In particular:

- The ALB target group is not automatically attached to the Kubernetes frontend service. Configure and validate an appropriate AWS Load Balancer Controller/target binding or choose one ingress mechanism.
- The in-cluster Redis example and Terraform ElastiCache instance are separate. Configure and test an authenticated encrypted Redis connection if using ElastiCache.
- Optional RDS is not used by the JSON account/key store. The backend remains one replica on local durable storage until a shared database is implemented.
- Container image publication, Kubernetes context, persistent volume provisioning, secrets, DNS/TLS, application deployment, restore procedures and rollout verification require explicit configuration and testing.

For an immediately runnable application, use the [project local instructions](../README.md). No cloud resource, blockchain transaction or trading operation is started by `terraform validate`.
