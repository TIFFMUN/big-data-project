variable "project" {
  description = "Project name prefix"
  type        = string
}

variable "bucket_arn" {
  description = "ARN of the S3 data lake bucket"
  type        = string
}

variable "tags" {
  description = "Common tags"
  type        = map(string)
  default     = {}
}

