variable "project" {
  description = "Project name prefix"
  type        = string
}

variable "bucket_name" {
  description = "S3 bucket name for Athena results"
  type        = string
}

variable "glue_database_name" {
  description = "Glue catalog database name"
  type        = string
}

variable "tags" {
  description = "Common tags"
  type        = map(string)
  default     = {}
}

