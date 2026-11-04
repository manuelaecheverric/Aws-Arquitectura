output "instance_id" {
  value = aws_instance.airflow.id
}

output "public_ip" {
  value = aws_instance.airflow.public_ip
}

output "ssh_command" {
  value = "ssh -i ~/.ssh/${var.key_name}.pem ubuntu@${aws_instance.airflow.public_ip}"
}

output "tunnel_command" {
  value = "ssh -i ~/.ssh/${var.key_name}.pem -L 8080:localhost:8080 ubuntu@${aws_instance.airflow.public_ip}"
}

output "stop_command" {
  value = "aws ec2 stop-instances --region ${var.region} --instance-ids ${aws_instance.airflow.id}"
}

output "start_command" {
  value = "aws ec2 start-instances --region ${var.region} --instance-ids ${aws_instance.airflow.id}"
}
