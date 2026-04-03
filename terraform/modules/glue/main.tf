###############################################################################
# AWS Glue – Data Catalog & Crawler
###############################################################################

resource "aws_glue_catalog_database" "data_lake" {
  name = "${replace(var.project, "-", "_")}_db"

  description = "Glue Data Catalog database for processed data"
}

resource "aws_glue_crawler" "processed" {
  name          = "${var.project}-processed-crawler"
  role          = var.glue_role_arn
  database_name = aws_glue_catalog_database.data_lake.name

  s3_target {
    path = "s3://${var.bucket_name}/processed/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  configuration = jsonencode({
    Version = 1.0
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
  })

  tags = var.tags
}

###############################################################################
# CloudWatch Log Group for Glue
###############################################################################

resource "aws_cloudwatch_log_group" "glue" {
  name              = "/aws-glue/crawlers/${var.project}"
  retention_in_days = 14

  tags = var.tags
}

