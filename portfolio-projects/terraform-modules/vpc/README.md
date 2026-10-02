# VPC Module

Reusable Terraform module for creating AWS VPC with public and private subnets.

Requires Terraform 1.7 or newer and the AWS provider 5.x or 6.x. It creates one NAT gateway when enabled; this is a demonstration topology, not a NAT gateway per availability zone. Supply an availability zone for each subnet and at least one public subnet when enabling NAT.

Private subnets always have explicit associations with a private route table. With `enable_nat_gateway = false`, that table has no internet default route and `private_route_table_id` remains available. A `moved` block preserves the existing route table address for users upgrading from the earlier counted resource.

## Validation

```sh
terraform init -backend=false -input=false
terraform fmt -check -recursive
terraform validate
terraform test
```

Mock-provider tests check private routing and subnet associations with and without NAT. They require no AWS credentials, create no cloud resources and do not establish that an AWS deployment has been tested.

## Usage

```hcl
module "vpc" {
  source = "../../terraform-modules/vpc"

  name                 = "my-project"
  vpc_cidr            = "10.0.0.0/16"
  availability_zones   = ["us-east-1a", "us-east-1b"]
  public_subnet_cidrs = ["10.0.101.0/24", "10.0.102.0/24"]
  private_subnet_cidrs = ["10.0.1.0/24", "10.0.2.0/24"]
  enable_nat_gateway  = true

  tags = {
    Environment = "dev"
  }
}
```

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|----------|
| name | Name prefix for resources | string | - | yes |
| vpc_cidr | CIDR block for VPC | string | - | yes |
| availability_zones | List of availability zones | list(string) | - | yes |
| public_subnet_cidrs | CIDR blocks for public subnets | list(string) | - | yes |
| private_subnet_cidrs | CIDR blocks for private subnets | list(string) | - | yes |
| enable_nat_gateway | Enable NAT Gateway | bool | true | no |
| enable_dns_hostnames | Enable DNS hostnames in VPC | bool | true | no |
| enable_dns_support | Enable DNS support in VPC | bool | true | no |
| tags | Additional tags for resources | map(string) | {} | no |

## Outputs

| Name | Description |
|------|-------------|
| vpc_id | VPC ID |
| vpc_cidr | VPC CIDR block |
| public_subnet_ids | Public subnet IDs |
| private_subnet_ids | Private subnet IDs |
| internet_gateway_id | Internet Gateway ID |
| nat_gateway_id | NAT Gateway ID |
| public_route_table_id | Public route table ID |
| private_route_table_id | Private route table ID |
| default_security_group_id | Default security group ID |
