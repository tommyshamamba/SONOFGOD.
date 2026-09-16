# Terraform Infrastructure for Kubernetes Demo

This Terraform configuration provisions AWS infrastructure for the Kubernetes Demo application, including EKS cluster, ALB, and supporting resources.

## Prerequisites

- Terraform >= 1.0
- AWS CLI configured with appropriate credentials
- AWS account with appropriate permissions
- S3 bucket for Terraform state

## Quick Start

### 1. Configure Backend

Update the backend configuration in `main.tf` or create a `backend.tf` file:

```hcl
terraform {
  backend "s3" {
    bucket         = "your-terraform-state-bucket"
    key            = "kubernetes-demo/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "your-terraform-locks-table"
  }
}
```

### 2. Create State Bucket and Lock Table

```bash
aws s3api create-bucket \
  --bucket your-terraform-state-bucket \
  --region us-east-1 \
  --create-bucket-configuration LocationConstraint=us-east-1

aws dynamodb create-table \
  --table-name your-terraform-locks-table \
  --attribute-definitions AttributeName=LockID,AttributeType=S \
  --key-schema AttributeName=LockID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region us-east-1
```

### 3. Configure Variables

Copy the example variables file:

```bash
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` with your specific values.

### 4. Initialize Terraform

```bash
terraform init
```

### 5. Plan and Apply

```bash
terraform plan
terraform apply
```

### 6. Configure kubectl

```bash
aws eks update-kubeconfig --name kubernetes-demo-cluster --region us-east-1
```

### 7. Deploy Kubernetes Resources

```bash
cd ..
kubectl apply -f k8s/
```

## Infrastructure Components

### Network
- **VPC**: 10.0.0.0/16 with public and private subnets across 2 AZs
- **NAT Gateway**: For private subnet internet access
- **Security Groups**: For ALB and EKS

### Compute
- **EKS Cluster**: Kubernetes 1.28 with managed node groups
- **Node Groups**: t3.small instances (2-4 nodes, auto-scaling)
- **Cluster Addons**: CoreDNS, kube-proxy, VPC-CNI, EBS CSI driver

### Networking
- **Application Load Balancer**: For frontend service
- **Target Groups**: HTTP/80 and HTTPS/443
- **SSL Certificate**: Use ACM certificate (configure via variable)

### Storage
- **EBS Storage Class**: GP3 storage for dynamic provisioning

### Monitoring
- **CloudWatch Log Groups**: For backend and frontend logs
- **Log Retention**: 7 days (configurable)

### Optional
- **ExternalDNS IAM Role**: For automatic DNS management

## Variables

### Optional Variables
- `aws_region`: AWS region for deployment (default: us-east-1)
- `environment`: Environment name (default: dev)
- `project_name`: Project name for resource naming
- `cluster_name`: EKS cluster name
- `kubernetes_version`: Kubernetes version
- `vpc_cidr`: VPC CIDR block
- `instance_types`: EC2 instance types
- `node_group_min_size/max_size/desired_size`: Node group scaling
- `acm_certificate_arn`: ACM certificate for HTTPS
- `log_retention_days`: CloudWatch log retention

See `variables.tf` for complete list.

## Outputs

After successful deployment, Terraform outputs important values:

- `vpc_id`, `vpc_cidr`: Network information
- `eks_cluster_id`, `eks_cluster_endpoint`: EKS cluster details
- `alb_dns_name`: Load balancer URL
- `kubeconfig_command`: Command to configure kubectl
- `external_dns_role_arn`: IAM role for ExternalDNS

## Cost Estimation

Approximate monthly costs (us-east-1, dev environment):

- EKS Cluster: $73/month
- EKS Nodes (2x t3.small): ~$40/month
- ALB: ~$22/month
- NAT Gateway: ~$32/month
- CloudWatch Logs: ~$5/month
- **Total**: ~$170/month

## Scaling

### Horizontal Pod Autoscaler
The Kubernetes HPA configuration automatically scales pods based on CPU/memory usage.

### Cluster Autoscaler
EKS node groups can be configured with cluster autoscaler for automatic node scaling.

## Security

### Best Practices Implemented
- VPC with private subnets for workloads
- Security groups with least privilege
- EBS encryption enabled
- No public access to cluster control plane

### Additional Security Recommendations
- Enable AWS GuardDuty
- Configure AWS Security Hub
- Enable VPC Flow Logs
- Implement network policies in Kubernetes
- Use IRSA (IAM Roles for Service Accounts)

## Troubleshooting

### Terraform State Lock
```bash
terraform force-unlock <LOCK_ID>
```

### EKS Cluster Access
```bash
aws eks update-kubeconfig --name kubernetes-demo-cluster --region us-east-1
kubectl get nodes
```

### Destroy Infrastructure
```bash
terraform destroy
```

## Production Considerations

1. **Multi-Environment**: Use separate Terraform workspaces
2. **State Management**: Use remote state with S3 and DynamoDB locks
3. **Monitoring**: Add CloudWatch alarms, Prometheus/Grafana
4. **CI/CD**: Integrate Terraform with GitHub Actions
5. **Cost Optimization**: Use Savings Plans for production

## Modules

This configuration uses Terraform modules:
- `terraform-aws-modules/vpc/aws`: VPC and networking
- `terraform-aws-modules/eks/aws`: EKS cluster
