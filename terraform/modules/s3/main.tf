###############################################################################
# S3 Data Lake
###############################################################################

resource "aws_s3_bucket" "data_lake" {
  bucket        = var.bucket_name
  force_destroy = false

  tags = merge(var.tags, {
    Name = var.bucket_name
  })

  # Keep the bucket (and its data) even when running terraform destroy
  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  versioning_configuration {
    status = "Enabled"
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true

  lifecycle {
    prevent_destroy = true
  }
}

# Create logical prefix placeholders
resource "aws_s3_object" "raw_prefix" {
  bucket  = aws_s3_bucket.data_lake.id
  key     = "raw/"
  content = ""
}

resource "aws_s3_object" "scripts_prefix" {
  bucket  = aws_s3_bucket.data_lake.id
  key     = "scripts/"
  content = ""
}

resource "aws_s3_object" "processed_prefix" {
  bucket  = aws_s3_bucket.data_lake.id
  key     = "processed/"
  content = ""
}

