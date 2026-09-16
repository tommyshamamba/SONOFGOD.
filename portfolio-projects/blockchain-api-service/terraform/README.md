# Terraform Infrastructure for Blockchain API Service

This Terraform configuration provisions AWS infrastructure for the Blockchain API Service, including EKS cluster, Redis, RDS PostgreSQL, ALB, and supporting resources.

## Prerequisites

- Terraform >= 1.0
- AWS CLI configured with appropriate credentials
- AWS account with appropriate permissions
- S3 bucket for Terraform state (create manually or update backend config)

## Quick Start

### 1. Configure Backend

Update the backend configuration in `main.tf` or create a `backend.tf` file:

```hcl
terraform {
  backend "s3" {
    bucket         = "your-terraform-state-bucket"
    key            = "blockchain-api-service/terraform.tfstate"
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

Edit `terraform.tfvars` with your specific values:

```hcl
aws_region  = "us-east-1"
environment = "dev"
database_password = "YourSecurePassword123!"
```

### 4. Initialize Terraform

```bash
terraform init
```

### 5. Plan and Apply

```bash
# Review the plan
terraform plan

# Apply the changes
terraform apply
```

### 6. Configure kubectl

After successful apply, configure kubectl:

```bash
aws eks update-kubeconfig --name blockchain-api-cluster --region us-east-1
```

### 7. Deploy Kubernetes Resources

```bash
cd ..
kubectl apply -f k8s/
```

## Infrastructure Components

### Network
- **VPC**: 10.0.0.0/16 with public and private subnets across 3 AZs
- **NAT Gateway**: For private subnet internet access
- **Security Groups**: For ALB, Redis, RDS, and EKS

### Compute
- **EKS Cluster**: Kubernetes 1.28 with managed node groups
- **Node Groups**: t3.medium instances (2-5 nodes, auto-scaling)
- **Cluster Addons**: CoreDNS, kube-proxy, VPC-CNI, EBS CSI driver

### Database
- **RDS PostgreSQL**: db.t3.micro with 20GB storage (auto-scaling to 100GB)
- **ElastiCache Redis**: cache.t3.micro single-node cluster
- **Encryption**: At-rest and in-transit encryption enabled

### Networking
- **Application Load Balancer**: For frontend service
- **Target Groups**: HTTP/80 and HTTPS/443
- **SSL Certificate**: Use ACM certificate (configure via variable)

### Storage
- **S3 Bucket**: For application assets (versioned, encrypted)
- **EBS Storage**: Via CSI driver for persistent volumes

### Monitoring
- **CloudWatch Log Groups**: For backend and frontend logs
- **Log Retention**: 7 days (configurable)

## Variables

### Required Variables
- `aws_region`: AWS region for deployment
- `database_password`: RDS database password

### Optional Variables
- `environment`: Environment name (default: dev)
- `project_name`: Project name for resource naming
- `cluster_name`: EKS cluster name
- `kubernetes_version`: Kubernetes version
- `vpc_cidr`: VPC CIDR block
- `instance_types`: EC2 instance types
- `node_group_min_size/max_size/desired_size`: Node group scaling
- `redis_node_type`: ElastiCache instance type
- `create_database`: Whether to create RDS
- `acm_certificate_arn`: ACM certificate for HTTPS
- `log_retention_days`: CloudWatch log retention

See `variables.tf` for complete list.

## Outputs

After successful deployment, Terraform outputs important values:

- `vpc_id`, `vpc_cidr`: Network information
- `eks_cluster_id`, `eks_cluster_endpoint`: EKS cluster details
- `redis_endpoint`, `redis_port`: Redis connection details
- `database_endpoint`, `database_port`: Database connection details
- `alb_dns_name`: Load balancer URL
- `kubeconfig_command`: Command to configure kubectl

## Cost Estimation

Approximate monthly costs (us-east-1, dev environment):

- EKS Cluster: $73/month
- EKS Nodes (2x t3.medium): ~$60/month
- RDS PostgreSQL (db.t3.micro): ~$15/month
- ElastiCache Redis (cache.t3.micro): ~$12/month
- ALB: ~$22/month
- NAT Gateway: ~$32/month
- CloudWatch Logs: ~$5/month
- **Total**: ~$220/month

Production costs will be higher based on scaling and usage.

## Scaling

### Horizontal Pod Autoscaler
The Kubernetes HPA configuration automatically scales pods based on CPU/memory usage.

### Cluster Autoscaler
EKS node groups can be configured with cluster autoscaler for automatic node scaling.

### Database Scaling
- RDS: Configure instance class and storage in variables
- Redis: Configure node type and cluster mode in variables

## Security

### Best Practices Implemented
- VPC with private subnets for workloads
- Security groups with least privilege
- Encryption at rest (RDS, ElastiCache, S3, EBS)
- Encryption in transit (TLS)
- No public access to databases
- IAM roles for service accounts (configure as needed)

### Additional Security Recommendations
- Enable AWS GuardDuty
- Configure AWS Security Hub
- Use AWS Secrets Manager for sensitive data
- Enable VPC Flow Logs
- Implement network policies in Kubernetes
- Use IRSA (IAM Roles for Service Accounts)

## Disaster Recovery

### Backups
- RDS: Automated backups enabled (7-day retention)
- EBS: EBS snapshots (configure backup policy)
- S3: Versioning enabled

### High Availability
- Multi-AZ deployment for RDS (configure in variables)
- Multi-AZ for Redis (configure cluster mode)
- EKS across multiple AZs

## Troubleshooting

### Terraform State Lock
If you encounter state lock issues:

```bash
terraform force-unlock <LOCK_ID>
```

### EKS Cluster Access
If kubectl cannot access the cluster:

```bash
aws eks update-kubeconfig --name blockchain-api-cluster --region us-east-1
kubectl get nodes
```

### Resource Limits
If you hit AWS service limits:
- Request limit increases in AWS Support Center
- Adjust instance types in variables

### Destroy Infrastructure

To destroy all resources:

```bash
terraform destroy
```

**Note**: This will delete all resources including databases and their data.

## Production Considerations

1. **Multi-Environment**: Use separate Terraform workspaces or directories for dev/staging/prod
2. **State Management**: Use remote state with S3 and DynamoDB locks
3. **Secrets Management**: Use AWS Secrets Manager or Parameter Store instead of variables
4. **Monitoring**: Add CloudWatch alarms, Prometheus/Grafana
5. **CI/CD**: Integrate Terraform with GitHub Actions or similar
6. **Compliance**: Enable AWS Config, CloudTrail
7. **Cost Optimization**: Use Savings Plans, Reserved Instances for production

## Modules

This configuration uses Terraform modules:
- `terraform-aws-modules/vpc/aws`: VPC and networking
- `terraform-aws-modules/eks/aws`: EKS cluster

## Support

For issues:
- Check Terraform logs: `terraform show`
- Check AWS CloudTrail for API call history
- Review CloudWatch logs for application issues
