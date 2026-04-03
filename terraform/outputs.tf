###############################################################################
# Root – outputs.tf
###############################################################################

# ── S3 ─────────────────────────────────────────────────────────────────────
output "s3_bucket_name" {
  description = "Data lake S3 bucket name"
  value       = module.s3.bucket_name
}

output "s3_bucket_arn" {
  description = "Data lake S3 bucket ARN"
  value       = module.s3.bucket_arn
}

# ── EC2 / Airflow ──────────────────────────────────────────────────────────
output "airflow_public_ip" {
  description = "Public IP of the Airflow EC2 instance"
  value       = module.ec2_airflow.airflow_public_ip
}

output "airflow_instance_id" {
  description = "Instance ID of the Airflow EC2"
  value       = module.ec2_airflow.airflow_instance_id
}

# ── EMR ────────────────────────────────────────────────────────────────────
output "emr_cluster_id" {
  description = "EMR cluster ID"
  value       = module.emr.cluster_id
}

output "emr_master_dns" {
  description = "EMR master node public DNS"
  value       = module.emr.master_public_dns
}

# ── Glue ───────────────────────────────────────────────────────────────────
output "glue_database_name" {
  description = "Glue Data Catalog database name"
  value       = module.glue.catalog_database_name
}

output "glue_crawler_name" {
  description = "Glue Crawler name"
  value       = module.glue.crawler_name
}

# ── Athena ─────────────────────────────────────────────────────────────────
output "athena_workgroup" {
  description = "Athena workgroup name"
  value       = module.athena.workgroup_name
}

# ── IAM ────────────────────────────────────────────────────────────────────
output "iam_ec2_airflow_role_arn" {
  description = "IAM role ARN for EC2 Airflow"
  value       = module.iam.ec2_airflow_role_arn
}

output "iam_emr_service_role_arn" {
  description = "IAM service role ARN for EMR"
  value       = module.iam.emr_service_role_arn
}

output "iam_glue_role_arn" {
  description = "IAM role ARN for Glue"
  value       = module.iam.glue_role_arn
}

output "iam_athena_role_arn" {
  description = "IAM role ARN for Athena"
  value       = module.iam.athena_role_arn
}


