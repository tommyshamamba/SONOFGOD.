terraform {
  required_version = ">= 1.0"
}

resource "aws_lb" "this" {
  name               = var.name
  internal           = var.internal
  load_balancer_type = var.load_balancer_type
  security_groups    = var.security_group_ids
  subnets            = var.subnet_ids

  enable_deletion_protection = var.enable_deletion_protection
  enable_http2               = var.enable_http2
  enable_cross_zone_load_balancing = var.enable_cross_zone_load_balancing

  dynamic "access_logs" {
    for_each = var.enable_access_logs ? [1] : []
    content {
      bucket  = var.access_logs_bucket
      prefix  = var.access_logs_prefix
      enabled = true
    }
  }

  tags = merge(
    {
      Name = var.name
    },
    var.tags
  )
}

resource "aws_lb_target_group" "this" {
  count = var.create_target_group ? 1 : 0

  name        = "${var.name}-tg"
  port        = var.target_group_port
  protocol    = var.target_group_protocol
  vpc_id      = var.vpc_id
  target_type = var.target_type

  dynamic "health_check" {
    for_each = var.health_check_enabled ? [1] : []
    content {
      enabled             = true
      path                = var.health_check_path
      interval            = var.health_check_interval
      timeout             = var.health_check_timeout
      healthy_threshold   = var.health_check_healthy_threshold
      unhealthy_threshold = var.health_check_unhealthy_threshold
      matcher             = var.health_check_matcher
    }
  }

  stickiness {
    type = var.stickiness_type
    cookie_duration = var.stickiness_cookie_duration
    enabled = var.stickiness_enabled
  }

  tags = merge(
    {
      Name = "${var.name}-tg"
    },
    var.tags
  )
}

resource "aws_lb_listener" "http" {
  count = var.create_http_listener ? 1 : 0

  load_balancer_arn = aws_lb.this.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type = var.http_listener_action_type
    
    dynamic "redirect" {
      for_each = var.http_listener_action_type == "redirect" ? [1] : []
      content {
        port        = "443"
        protocol    = "HTTPS"
        status_code = "HTTP_301"
      }
    }

    dynamic "forward" {
      for_each = var.http_listener_action_type == "forward" && var.create_target_group ? [1] : []
      content {
        target_group_arn = aws_lb_target_group.this[0].arn
      }
    }
  }
}

resource "aws_lb_listener" "https" {
  count = var.create_https_listener && var.acm_certificate_arn != "" ? 1 : 0

  load_balancer_arn = aws_lb.this.arn
  port              = 443
  protocol          = "HTTPS"
  ssl_policy        = var.ssl_policy
  certificate_arn   = var.acm_certificate_arn

  default_action {
    type = "forward"
    target_group_arn = var.create_target_group ? aws_lb_target_group.this[0].arn : null
  }
}

resource "aws_security_group" "alb" {
  count = var.create_security_group ? 1 : 0

  name_prefix = "${var.name}-"
  description = var.security_group_description
  vpc_id      = var.vpc_id

  dynamic "ingress" {
    for_each = var.ingress_rules
    content {
      from_port   = ingress.value.from_port
      to_port     = ingress.value.to_port
      protocol    = ingress.value.protocol
      cidr_blocks = ingress.value.cidr_blocks
    }
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(
    {
      Name = "${var.name}-sg"
    },
    var.tags
  )

  lifecycle {
    create_before_destroy = true
  }
}
