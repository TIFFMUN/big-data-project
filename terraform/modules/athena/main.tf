###############################################################################
# AWS Athena – Query Layer
###############################################################################

resource "aws_athena_workgroup" "main" {
  name = "${var.project}-workgroup"

  configuration {
    enforce_workgroup_configuration = true

    result_configuration {
      output_location = "s3://${var.bucket_name}/athena-results/"
    }
  }

  tags = var.tags
}

resource "aws_athena_named_query" "sample" {
  name        = "${var.project}-sample-query"
  workgroup   = aws_athena_workgroup.main.name
  database    = var.glue_database_name
  description = "Sample query – list all tables in the data catalog"
  query       = "SHOW TABLES IN ${var.glue_database_name};"
}

