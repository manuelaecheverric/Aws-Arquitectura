# 3.1 Fuentes: MySQL local -> Amazon RDS for MySQL; MongoDB local -> Amazon DocumentDB.
# Ninguna es pública; se cifran en reposo; las contraseñas viven en Secrets Manager.

# ------------------------------------------------------------------ RDS MySQL
resource "aws_db_subnet_group" "mysql" {
  name       = "${var.prefix}-mysql-subnets"
  subnet_ids = data.aws_subnets.default.ids
}

resource "aws_db_instance" "mysql" {
  identifier                  = "${var.prefix}-mysql"
  engine                      = "mysql"
  engine_version              = "8.0"
  instance_class              = var.rds_instance_class
  allocated_storage           = 20
  storage_type                = "gp3"
  storage_encrypted           = true
  db_name                     = "ecommerce"
  username                    = "dwadmin"
  manage_master_user_password = true # RDS genera la clave y la guarda en Secrets Manager
  db_subnet_group_name        = aws_db_subnet_group.mysql.name
  vpc_security_group_ids      = [aws_security_group.sources.id]
  publicly_accessible         = false
  multi_az                    = false
  backup_retention_period     = 1
  skip_final_snapshot         = true # taller: al destruir no se deja snapshot que cobre
  deletion_protection         = false
  apply_immediately           = true
  auto_minor_version_upgrade  = true
}

# ---------------------------------------------------------------- DocumentDB
resource "random_password" "docdb" {
  length           = 24
  special          = true
  override_special = "_-" # DocumentDB no admite / @ " ni espacios en la clave
}

resource "aws_secretsmanager_secret" "docdb" {
  name                    = "${var.prefix}/docdb/master"
  description             = "Credenciales del usuario maestro de DocumentDB (taller DW)"
  recovery_window_in_days = 0 # taller: borrado inmediato al destruir
}

resource "aws_secretsmanager_secret_version" "docdb" {
  secret_id = aws_secretsmanager_secret.docdb.id
  secret_string = jsonencode({
    username = "dwadmin"
    password = random_password.docdb.result
  })
}

resource "aws_docdb_subnet_group" "this" {
  name       = "${var.prefix}-docdb-subnets"
  subnet_ids = data.aws_subnets.default.ids
}

resource "aws_docdb_cluster" "this" {
  cluster_identifier      = "${var.prefix}-docdb"
  engine                  = "docdb"
  engine_version          = "5.0.0"
  master_username         = "dwadmin"
  master_password         = random_password.docdb.result
  db_subnet_group_name    = aws_docdb_subnet_group.this.name
  vpc_security_group_ids  = [aws_security_group.sources.id]
  storage_encrypted       = true
  backup_retention_period = 1
  skip_final_snapshot     = true
  deletion_protection     = false
  apply_immediately       = true
}

resource "aws_docdb_cluster_instance" "this" {
  identifier         = "${var.prefix}-docdb-1"
  cluster_identifier = aws_docdb_cluster.this.id
  instance_class     = var.docdb_instance_class
  apply_immediately  = true
}
