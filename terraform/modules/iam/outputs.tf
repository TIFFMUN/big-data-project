output "ec2_airflow_instance_profile_name" {
  value = aws_iam_instance_profile.ec2_airflow.name
}

output "ec2_airflow_role_arn" {
  value = aws_iam_role.ec2_airflow.arn
}

output "emr_service_role_arn" {
  value = aws_iam_role.emr_service.arn
}

output "emr_service_role_name" {
  value = aws_iam_role.emr_service.name
}

output "emr_ec2_instance_profile_arn" {
  value = aws_iam_instance_profile.emr_ec2.arn
}

output "emr_ec2_instance_profile_name" {
  value = aws_iam_instance_profile.emr_ec2.name
}

output "emr_ec2_role_arn" {
  value = aws_iam_role.emr_ec2.arn
}

output "glue_role_arn" {
  value = aws_iam_role.glue.arn
}

output "athena_role_arn" {
  value = aws_iam_role.athena.arn
}

