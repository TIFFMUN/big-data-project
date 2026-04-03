output "airflow_instance_id" {
  value = aws_instance.airflow.id
}

output "airflow_public_ip" {
  value = aws_instance.airflow.public_ip
}

output "airflow_security_group_id" {
  value = aws_security_group.airflow.id
}

