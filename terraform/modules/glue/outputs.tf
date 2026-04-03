output "catalog_database_name" {
  value = aws_glue_catalog_database.data_lake.name
}

output "crawler_name" {
  value = aws_glue_crawler.processed.name
}

