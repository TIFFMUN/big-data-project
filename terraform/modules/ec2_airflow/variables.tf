variable "project" {
  description = "Project name prefix"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID"
  type        = string
}

variable "subnet_id" {
  description = "Subnet ID for the EC2 instance"
  type        = string
}

variable "instance_type" {
  description = "EC2 instance type"
  type        = string
  default     = "t3.medium"
}

variable "instance_profile_name" {
  description = "IAM instance profile name for the Airflow EC2"
  type        = string
}

variable "key_name" {
  description = "EC2 key pair name for SSH access"
  type        = string
  default     = ""
}

variable "allowed_cidr" {
  description = "CIDR block allowed to access Airflow UI and SSH"
  type        = string
  default     = "0.0.0.0/0"
}

variable "tags" {
  description = "Common tags"
  type        = map(string)
  default     = {}
}

