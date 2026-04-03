variable "bucket_name" {
  description = "Name of the S3 data lake bucket"
  type        = string
}

variable "tags" {
  description = "Common tags"
  type        = map(string)
  default     = {}
}

