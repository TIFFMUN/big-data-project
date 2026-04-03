###############################################################################
# Terraform Configuration & Provider
###############################################################################

terraform {
  required_version = "~> 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # ── S3 Remote Backend with DynamoDB Locking ───────────────────────────────
  # Uncomment below for team use. First create the S3 bucket + DynamoDB table
  # manually (or via a separate bootstrap TF config), then run terraform init.
  #
  # backend "s3" {
  #   bucket         = "your-terraform-state-bucket"
  #   key            = "big-data-project/terraform.tfstate"
  #   region         = "us-east-1"
  #   encrypt        = true
  #   dynamodb_table = "terraform-locks"   # prevents concurrent applies
  # }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = var.tags
  }
}

###############################################################################
# Data Sources – Default VPC & Subnets
###############################################################################

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

###############################################################################
# Module – S3 Data Lake
###############################################################################

module "s3" {
  source = "./modules/s3"

  bucket_name = var.bucket_name
  tags        = var.tags
}

###############################################################################
# Module – IAM Roles
###############################################################################

module "iam" {
  source = "./modules/iam"

  project    = var.project
  bucket_arn = module.s3.bucket_arn
  tags       = var.tags
}

###############################################################################
# Module – EC2 Airflow
###############################################################################

module "ec2_airflow" {
  source = "./modules/ec2_airflow"

  project               = var.project
  vpc_id                = data.aws_vpc.default.id
  subnet_id             = data.aws_subnets.default.ids[0]
  instance_type         = var.airflow_instance_type
  instance_profile_name = module.iam.ec2_airflow_instance_profile_name
  key_name              = var.key_name
  allowed_cidr          = var.allowed_cidr
  tags                  = var.tags
}

###############################################################################
# Module – EMR Cluster
###############################################################################

module "emr" {
  source = "./modules/emr"

  project                       = var.project
  vpc_id                        = data.aws_vpc.default.id
  subnet_id                     = data.aws_subnets.default.ids[0]
  emr_service_role_name         = module.iam.emr_service_role_name
  emr_ec2_instance_profile_name = module.iam.emr_ec2_instance_profile_name
  bucket_name                   = module.s3.bucket_name
  airflow_sg_id                 = module.ec2_airflow.airflow_security_group_id
  key_name                      = var.key_name
  master_instance_type          = var.emr_master_instance_type
  core_instance_type            = var.emr_core_instance_type
  task_instance_type            = var.emr_task_instance_type
  idle_timeout                  = var.emr_idle_timeout
  tags                          = var.tags
}

###############################################################################
# Module – Glue (Crawler + Data Catalog)
###############################################################################

module "glue" {
  source = "./modules/glue"

  project       = var.project
  bucket_name   = module.s3.bucket_name
  glue_role_arn = module.iam.glue_role_arn
  tags          = var.tags
}

###############################################################################
# Module – Athena (Query Layer)
###############################################################################

module "athena" {
  source = "./modules/athena"

  project            = var.project
  bucket_name        = module.s3.bucket_name
  glue_database_name = module.glue.catalog_database_name
  tags               = var.tags
}


