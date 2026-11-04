# Red: se usa la VPC por defecto de la cuenta y tres security groups propios.
#   ec2      -> la máquina de Airflow/dbt/scripts. Sin SSH abierto a internet.
#   sources  -> RDS y DocumentDB. Solo aceptan conexiones desde el SG de la EC2.
#   SSH      -> por EC2 Instance Connect (llave temporal de 60 s) SOLO desde la IP de CloudShell.
#               Se intentó un EC2 Instance Connect Endpoint (sin IP pública), pero requiere
#               iam:CreateServiceLinkedRole para ec2-instance-connect, que el grupo no permite.

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

resource "aws_security_group" "ec2" {
  name        = "${var.prefix}-ec2-sg"
  description = "EC2 de Airflow, dbt y scripts del taller DW"
  vpc_id      = data.aws_vpc.default.id
}

resource "aws_vpc_security_group_ingress_rule" "ec2_ssh_admin" {
  security_group_id = aws_security_group.ec2.id
  cidr_ipv4         = var.ssh_allowed_cidr
  ip_protocol       = "tcp"
  from_port         = 22
  to_port           = 22
  description       = "SSH solo desde la IP de administracion (CloudShell)"
}

resource "aws_vpc_security_group_egress_rule" "ec2_all" {
  security_group_id = aws_security_group.ec2.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
  description       = "Salida a internet (paquetes) y a los servicios de AWS"
}

resource "aws_security_group" "sources" {
  name        = "${var.prefix}-sources-sg"
  description = "RDS MySQL y DocumentDB: solo desde la EC2 del taller"
  vpc_id      = data.aws_vpc.default.id
}

resource "aws_vpc_security_group_ingress_rule" "mysql_from_ec2" {
  security_group_id            = aws_security_group.sources.id
  referenced_security_group_id = aws_security_group.ec2.id
  ip_protocol                  = "tcp"
  from_port                    = 3306
  to_port                      = 3306
}

resource "aws_vpc_security_group_ingress_rule" "docdb_from_ec2" {
  security_group_id            = aws_security_group.sources.id
  referenced_security_group_id = aws_security_group.ec2.id
  ip_protocol                  = "tcp"
  from_port                    = 27017
  to_port                      = 27017
}

resource "aws_vpc_security_group_ingress_rule" "airflow_ui" {
  count             = var.airflow_ui_cidr == "" ? 0 : 1
  security_group_id = aws_security_group.ec2.id
  cidr_ipv4         = var.airflow_ui_cidr
  ip_protocol       = "tcp"
  from_port         = 8080
  to_port           = 8080
  description       = "Interfaz de Airflow solo desde la IP autorizada"
}
