# Mocked resources only: no AWS credentials or live infrastructure are used.
mock_provider "aws" {
  mock_resource "aws_lb" {
    defaults = {
      arn = "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/portfolio-test/1234567890123456"
    }
  }
  mock_resource "aws_lb_target_group" {
    defaults = {
      arn = "arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/portfolio-test/1234567890123456"
    }
  }
  mock_resource "aws_security_group" {
    defaults = { id = "sg-0123456789abcdef0" }
  }
}

variables {
  name                = "portfolio-test"
  vpc_id              = "vpc-0123456789abcdef0"
  subnet_ids          = ["subnet-0123456789abcdef0", "subnet-0123456789abcdef1"]
  acm_certificate_arn = "arn:aws:acm:us-east-1:123456789012:certificate/12345678-1234-1234-1234-123456789012"
}

run "https_attaches_created_and_existing_security_groups" {
  command = apply

  variables {
    security_group_ids = ["sg-0123456789abcdef1"]
  }

  assert {
    condition     = toset(aws_lb.this.security_groups) == toset([aws_security_group.alb[0].id, "sg-0123456789abcdef1"])
    error_message = "The ALB must attach both its created security group and caller-supplied groups."
  }
  assert {
    condition     = aws_lb_listener.http[0].default_action[0].type == "redirect" && aws_lb_listener.http[0].default_action[0].redirect[0].protocol == "HTTPS"
    error_message = "The secure configuration must redirect HTTP traffic to HTTPS."
  }
  assert {
    condition     = aws_lb_listener.https[0].default_action[0].target_group_arn == aws_lb_target_group.this[0].arn
    error_message = "The HTTPS listener must forward to the created target group."
  }
}

run "http_only_requires_explicit_forwarding" {
  command = apply

  variables {
    create_security_group     = false
    security_group_ids        = ["sg-0123456789abcdef1"]
    create_https_listener     = false
    acm_certificate_arn       = ""
    http_listener_action_type = "forward"
  }

  assert {
    condition     = length(aws_security_group.alb) == 0 && toset(aws_lb.this.security_groups) == toset(["sg-0123456789abcdef1"])
    error_message = "Disabling security group creation must preserve supplied groups."
  }
  assert {
    condition     = length(aws_lb_listener.https) == 0 && aws_lb_listener.http[0].default_action[0].type == "forward"
    error_message = "The explicit HTTP-only configuration must forward without a dangling HTTPS redirect."
  }
}

run "reject_https_without_certificate" {
  command = plan
  variables {
    acm_certificate_arn = ""
  }
  expect_failures = [aws_lb_listener.http, aws_lb_listener.https]
}

run "reject_redirect_without_https_listener" {
  command = plan
  variables {
    create_https_listener = false
  }
  expect_failures = [aws_lb_listener.http]
}

run "reject_forwarding_without_target_group" {
  command = plan
  variables {
    create_target_group       = false
    create_https_listener     = false
    http_listener_action_type = "forward"
  }
  expect_failures = [aws_lb_listener.http]
}

run "reject_missing_security_groups" {
  command = plan
  variables {
    create_security_group = false
  }
  expect_failures = [aws_lb.this]
}
