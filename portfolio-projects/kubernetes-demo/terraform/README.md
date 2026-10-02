# Kubernetes Demo infrastructure example

This Terraform configuration describes an AWS VPC, EKS cluster and managed nodes, an ALB, log groups, an EBS storage class and an ExternalDNS IAM role. It is an infrastructure example, separate from the [runnable local application](../README.md). No AWS deployment or end-to-end cloud rollout has been verified.

## Validate without creating resources

From this directory, with Terraform installed:

```sh
terraform init -backend=false -input=false
terraform fmt -check
terraform validate
```

Initialization downloads the pinned modules and providers; disabling the backend avoids accessing remote state. Schema validation does not need AWS credentials, create resources or test AWS availability. Commit `.terraform.lock.hcl`, and keep `.terraform/`, private variable files and state out of Git.

## Configuration before a sandbox plan

1. Edit the existing S3 backend in `main.tf` to reference your own state bucket and lock table. Create those separately and restrict access to state. Do not add a duplicate backend block.
2. Copy `terraform.tfvars.example` to a private `terraform.tfvars` file. Supply `kubernetes_version` explicitly after checking support in the chosen AWS region. There is no fixed version default because regional availability and support change.
3. Review the pinned EKS/VPC modules, provider versions, node image compatibility, permissions and subnet choices against the selected EKS version. Successful validation does not establish compatibility with a live cluster.
4. If enabling HTTPS, provide an ACM certificate from the ALB's region. The empty certificate setting creates an HTTP-only listener for the example; it is unsuitable for credentials or sensitive traffic.
5. Review a plan in an isolated account before any apply. EKS, nodes, the ALB, NAT gateway and logs can incur charges. This repository does not establish a current cost estimate.

The configuration enables both public and private EKS API endpoints. The public endpoint is not restricted to particular client CIDRs by this example. Restrict access or disable the public endpoint before a real deployment, and ensure the Terraform runner can reach the selected API endpoint. The Kubernetes provider needs the AWS CLI; it requests tokens using the actual cluster name and configured region.

## Cloud integration still required

- The Terraform ALB target group has no automatic connection to the Kubernetes frontend service. Choose and configure an ingress approach, such as an AWS Load Balancer Controller target binding. The local `LoadBalancer` service and Terraform ALB are separate resources.
- The EBS CSI addon and storage class need suitable IAM permissions for volume provisioning. The configuration does not create an addon service-account IAM role or prove volume attachment works.
- Creating log groups does not ship container logs. Add and verify a log collector before claiming application observability.
- The ExternalDNS role is an IAM example only; ExternalDNS itself is not installed. Review and restrict its hosted-zone permissions before use.
- Application images, secrets, metrics-server/HPA behavior, DNS, TLS, Kubernetes access, network policies and rollout verification require configuration and testing.

The EKS node group defines minimum, desired and maximum sizes; automatic node scaling requires a separately configured autoscaler. The application HPA scales pods only when a metrics API is available.

## Useful outputs

`eks_cluster_name`, `eks_cluster_endpoint`, `vpc_id`, subnet IDs and `kubeconfig_command` help connect subsequent deployment steps. `alb_dns_name` identifies the standalone ALB, not a verified working application URL. The legacy `eks_cluster_id` output is populated only for EKS on Outposts; use `eks_cluster_name` for regional clusters.

For a credential-free application demonstration, follow the local Node.js or Docker Compose instructions in the [project README](../README.md).
