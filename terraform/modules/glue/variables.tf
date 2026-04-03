variable "project" {
  description = "Project name prefix"
  type        = string
}

variable "bucket_name" {
  description = "S3 bucket name"
  type        = string
}

variable "glue_role_arn" {
  description = "IAM role ARN for Glue"
  type        = string
}

variable "tags" {
  description = "Common tags"
  type        = map(string)
  default     = {}
}

