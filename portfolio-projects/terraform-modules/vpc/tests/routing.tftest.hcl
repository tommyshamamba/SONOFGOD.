# Mocked resources only: these tests never create a VPC or NAT gateway in AWS.
mock_provider "aws" {
  mock_resource "aws_vpc" {
    defaults = { id = "vpc-0123456789abcdef0" }
  }
  mock_resource "aws_subnet" {
    defaults = { id = "subnet-0123456789abcdef0" }
  }
  mock_resource "aws_route_table" {
    defaults = { id = "rtb-0123456789abcdef0" }
  }
  mock_resource "aws_nat_gateway" {
    defaults = { id = "nat-0123456789abcdef0" }
  }
}

variables {
  name                 = "portfolio-test"
  vpc_cidr             = "10.0.0.0/16"
  availability_zones   = ["us-east-1a", "us-east-1b"]
  public_subnet_cidrs  = ["10.0.101.0/24", "10.0.102.0/24"]
  private_subnet_cidrs = ["10.0.1.0/24", "10.0.2.0/24"]
}

run "private_subnets_stay_associated_without_nat" {
  command = apply
  variables {
    enable_nat_gateway = false
  }

  assert {
    condition     = length(aws_nat_gateway.this) == 0 && length(aws_eip.nat) == 0
    error_message = "Disabling NAT must not allocate a NAT gateway or elastic IP."
  }
  assert {
    condition     = length(aws_route_table_association.private) == 2 && alltrue([for association in aws_route_table_association.private : association.route_table_id == aws_route_table.private.id])
    error_message = "Each private subnet must explicitly use the private route table even without NAT."
  }
  assert {
    condition     = length(aws_route_table.private.route) == 0
    error_message = "The isolated private route table must have no default internet route."
  }
  assert {
    condition     = output.private_route_table_id == aws_route_table.private.id
    error_message = "A private route table ID must remain available without NAT."
  }
}

run "nat_adds_private_default_route" {
  command = apply

  assert {
    condition     = length(aws_nat_gateway.this) == 1 && length(aws_eip.nat) == 1
    error_message = "The single-NAT topology must allocate exactly one gateway and elastic IP."
  }
  assert {
    condition     = anytrue([for route in aws_route_table.private.route : route.cidr_block == "0.0.0.0/0" && route.nat_gateway_id == aws_nat_gateway.this[0].id])
    error_message = "Private internet egress must use the NAT gateway rather than the public internet gateway."
  }
  assert {
    condition     = length(aws_route_table_association.private) == 2
    error_message = "Both private subnets must retain explicit route table associations."
  }
}

run "disabling_nat_removes_existing_default_route" {
  command = apply
  variables {
    enable_nat_gateway = false
  }

  assert {
    condition     = length(aws_route_table.private.route) == 0 && length(aws_nat_gateway.this) == 0
    error_message = "Disabling NAT after an earlier apply must remove the managed internet route and gateway."
  }
  assert {
    condition     = length(aws_route_table_association.private) == 2
    error_message = "Removing NAT must preserve private subnet routing associations."
  }
}
