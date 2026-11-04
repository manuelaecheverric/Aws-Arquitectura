# dbt necesita conexión directa a Redshift (puerto 5439). El workgroup se creó por consola
# con el security group "default"; este SG propio solo acepta 5439 desde la EC2 del taller
# y se agrega al workgroup (aws redshift-serverless update-workgroup --security-group-ids ...).
resource "aws_security_group" "redshift" {
  name        = "${var.prefix}-redshift-sg"
  description = "Redshift Serverless del taller: 5439 solo desde la EC2"
  vpc_id      = data.aws_vpc.default.id
}

resource "aws_vpc_security_group_ingress_rule" "redshift_from_ec2" {
  security_group_id            = aws_security_group.redshift.id
  referenced_security_group_id = aws_security_group.ec2.id
  ip_protocol                  = "tcp"
  from_port                    = 5439
  to_port                      = 5439
}

output "redshift_sg_id" { value = aws_security_group.redshift.id }
