###############################################################################
# EMR Cluster – PySpark Processing
###############################################################################

resource "aws_security_group" "emr_master" {
  name        = "${var.project}-emr-master-sg"
  description = "Security group for EMR master node"
  vpc_id      = var.vpc_id

  revoke_rules_on_delete = true

  ingress {
    description     = "SSH from Airflow SG"
    from_port       = 22
    to_port         = 22
    protocol        = "tcp"
    security_groups = [var.airflow_sg_id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name                                       = "${var.project}-emr-master-sg"
    "for-use-with-amazon-emr-managed-policies" = "true"
  })
}

resource "aws_security_group" "emr_core" {
  name        = "${var.project}-emr-core-sg"
  description = "Security group for EMR core/task nodes"
  vpc_id      = var.vpc_id

  revoke_rules_on_delete = true

  ingress {
    description     = "All traffic from master"
    from_port       = 0
    to_port         = 0
    protocol        = "-1"
    security_groups = [aws_security_group.emr_master.id]
  }

  ingress {
    description = "All traffic from self (core-to-core)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name                                       = "${var.project}-emr-core-sg"
    "for-use-with-amazon-emr-managed-policies" = "true"
  })
}

# Allow master to talk to core nodes
resource "aws_security_group_rule" "master_to_core" {
  type                     = "ingress"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
  security_group_id        = aws_security_group.emr_master.id
  source_security_group_id = aws_security_group.emr_core.id
}

resource "aws_emr_cluster" "spark" {
  name          = "${var.project}-emr-cluster"
  release_label = var.emr_release
  applications  = ["Spark"]
  service_role  = var.emr_service_role_name

  log_uri = "s3://${var.bucket_name}/emr-logs/"

  ec2_attributes {
    instance_profile                  = var.emr_ec2_instance_profile_name
    subnet_id                         = var.subnet_id
    emr_managed_master_security_group = aws_security_group.emr_master.id
    emr_managed_slave_security_group  = aws_security_group.emr_core.id
    key_name                          = var.key_name
  }

  # Master – 1 node
  master_instance_group {
    name           = "Master"
    instance_type  = var.master_instance_type
    instance_count = 1
  }

  # Core – 2 nodes
  core_instance_group {
    name           = "Core"
    instance_type  = var.core_instance_type
    instance_count = 2
  }

  # Auto-termination after idle
  auto_termination_policy {
    idle_timeout = var.idle_timeout
  }

  configurations_json = jsonencode([
    {
      Classification = "spark-defaults"
      Properties = {
        "spark.dynamicAllocation.enabled" = "true"
      }
    }
  ])

  tags = merge(var.tags, {
    Name = "${var.project}-emr-cluster"
  })
}

# Task instance group – 1 node
resource "aws_emr_instance_group" "task" {
  cluster_id     = aws_emr_cluster.spark.id
  name           = "Task"
  instance_type  = var.task_instance_type
  instance_count = 1
}

###############################################################################
# CloudWatch Log Group for EMR
###############################################################################

resource "aws_cloudwatch_log_group" "emr" {
  name              = "/aws/emr/${var.project}"
  retention_in_days = 14

  tags = var.tags
}

