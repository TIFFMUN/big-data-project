###############################################################################
# Root – variables.tf
###############################################################################

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Project name used as a prefix for all resources"
  type        = string
  default     = "bigdata-project"
}

variable "bucket_name" {
  description = "Name of the S3 data lake bucket (must be globally unique)"
  type        = string
  default     = "bigdata-project-data-lake"
}

variable "key_name" {
  description = "EC2 key pair name for SSH access (leave empty to skip)"
  type        = string
  default     = ""
}

variable "allowed_cidr" {
  description = "CIDR block allowed to access Airflow UI / SSH"
  type        = string
  default     = "0.0.0.0/0"
}

variable "airflow_instance_type" {
  description = "EC2 instance type for Airflow"
  type        = string
  default     = "t3.micro"
}

variable "emr_master_instance_type" {
  description = "Instance type for EMR master node"
  type        = string
  default     = "m5.xlarge"
}

variable "emr_core_instance_type" {
  description = "Instance type for EMR core nodes"
  type        = string
  default     = "m5.xlarge"
}

variable "emr_task_instance_type" {
  description = "Instance type for EMR task node"
  type        = string
  default     = "m5.xlarge"
}

variable "emr_idle_timeout" {
  description = "Seconds of idle time before EMR auto-terminates"
  type        = number
  default     = 900
}


variable "tags" {
  description = "Common tags applied to all resources"
  type        = map(string)
  default = {
    Project     = "big-data-project"
    ManagedBy   = "terraform"
    Environment = "dev"
  }
}

