###############################################################################
# EC2 – Airflow Orchestrator
###############################################################################

data "aws_ami" "amazon_linux" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["amzn2-ami-hvm-*-x86_64-gp2"]
  }

  filter {
    name   = "state"
    values = ["available"]
  }
}

resource "aws_security_group" "airflow" {
  name        = "${var.project}-airflow-sg"
  description = "Security group for Airflow EC2 instance"
  vpc_id      = var.vpc_id

  # Airflow web UI
  ingress {
    description = "Airflow Web UI"
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr]
  }

  # SSH
  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.project}-airflow-sg"
  })
}

resource "aws_instance" "airflow" {
  ami                    = data.aws_ami.amazon_linux.id
  instance_type          = var.instance_type
  subnet_id              = var.subnet_id
  iam_instance_profile   = var.instance_profile_name
  vpc_security_group_ids = [aws_security_group.airflow.id]
  key_name               = var.key_name

  root_block_device {
    volume_size = 30
    volume_type = "gp3"
  }

  user_data = <<-EOF
    #!/bin/bash
    set -e

    # ── Install Docker ─────────────────────────────────────────────────────
    yum update -y
    amazon-linux-extras install docker -y
    systemctl enable docker && systemctl start docker
    usermod -aG docker ec2-user

    # ── Install Docker Compose v2 ──────────────────────────────────────────
    mkdir -p /usr/local/lib/docker/cli-plugins
    curl -SL "https://github.com/docker/compose/releases/download/v2.29.1/docker-compose-linux-x86_64" \
      -o /usr/local/lib/docker/cli-plugins/docker-compose
    chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

    # ── Install git to clone project ───────────────────────────────────────
    yum install -y git

    # ── Install CloudWatch agent ───────────────────────────────────────────
    yum install -y amazon-cloudwatch-agent
    cat > /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json <<'CW'
    {
      "logs": {
        "logs_collected": {
          "files": {
            "collect_list": [
              {
                "file_path": "/home/ec2-user/big-data-project/airflow/logs/**",
                "log_group_name": "/aws/ec2/${var.project}-airflow",
                "log_stream_name": "{instance_id}/airflow"
              }
            ]
          }
        }
      }
    }
    CW
    /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
      -a fetch-config -m ec2 \
      -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json -s

    # ── Bootstrap Airflow (will be started by user via docker compose) ─────
    mkdir -p /home/ec2-user/big-data-project/airflow/{dags,logs,plugins,scripts}
    chown -R ec2-user:ec2-user /home/ec2-user/big-data-project
  EOF

  tags = merge(var.tags, {
    Name = "${var.project}-airflow"
  })
}

###############################################################################
# CloudWatch Log Group
###############################################################################

resource "aws_cloudwatch_log_group" "airflow" {
  name              = "/aws/ec2/${var.project}-airflow"
  retention_in_days = 14

  tags = var.tags
}


