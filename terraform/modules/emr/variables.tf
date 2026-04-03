variable "project" {
  description = "Project name prefix"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID"
  type        = string
}

variable "subnet_id" {
  description = "Subnet ID for EMR cluster"
  type        = string
}

variable "emr_release" {
  description = "EMR release label"
  type        = string
  default     = "emr-6.15.0"
}

variable "master_instance_type" {
  description = "Instance type for master node"
  type        = string
  default     = "m5.xlarge"
}

variable "core_instance_type" {
  description = "Instance type for core nodes"
  type        = string
  default     = "m5.xlarge"
}

variable "task_instance_type" {
  description = "Instance type for task node"
  type        = string
  default     = "m5.xlarge"
}

variable "emr_service_role_name" {
  description = "EMR service IAM role name"
  type        = string
}

variable "emr_ec2_instance_profile_name" {
  description = "EMR EC2 instance profile name"
  type        = string
}

variable "bucket_name" {
  description = "S3 bucket name for logs and data"
  type        = string
}

variable "airflow_sg_id" {
  description = "Security group ID of the Airflow EC2 instance"
  type        = string
}

variable "key_name" {
  description = "EC2 key pair name"
  type        = string
  default     = ""
}

variable "idle_timeout" {
  description = "Seconds of idle time before auto-termination"
  type        = number
  default     = 3600
}

variable "tags" {
  description = "Common tags"
  type        = map(string)
  default     = {}
}

