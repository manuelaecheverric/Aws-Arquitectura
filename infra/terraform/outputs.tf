output "ec2_instance_id" { value = aws_instance.pipeline.id }
output "mysql_endpoint" { value = aws_db_instance.mysql.address }
output "mysql_secret_arn" { value = aws_db_instance.mysql.master_user_secret[0].secret_arn }
output "docdb_endpoint" { value = aws_docdb_cluster.this.endpoint }
output "docdb_secret_arn" { value = aws_secretsmanager_secret.docdb.arn }
output "ec2_role_arn" { value = aws_iam_role.ec2.arn }
