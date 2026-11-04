# Recursos creados por CONSOLA en el taller e importados a Terraform (ver imports.tf).
# Generado con `terraform plan -generate-config-out=console_resources.tf` y ajustado a mano:
#  - namespace: se quitó kms_key_id = "AWS_OWNED_KMS_KEY" (no es un ARN válido para el proveedor)
#    y se ignoran los atributos de la contraseña administrada por Secrets Manager.
#  - workgroup: se ignoran config_parameter (un parámetro nuevo de Redshift no está en el proveedor)
#    y price_performance_target (desactivado, nivel 0 no es válido en el proveedor).
# Verificado: después del import, `terraform plan` = "No changes".

# ------------------------------------------------------------------ S3 (3.2)
resource "aws_s3_bucket" "dw" {
  bucket              = "dw-user3-ecommerce-20261027"
  object_lock_enabled = false
}

resource "aws_s3_bucket_versioning" "dw" {
  bucket = aws_s3_bucket.dw.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "dw" {
  bucket = aws_s3_bucket.dw.id
  rule {
    bucket_key_enabled = true
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "dw" {
  bucket                  = aws_s3_bucket.dw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_policy" "dw" {
  bucket = aws_s3_bucket.dw.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "SoloConexionesCifradas"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = ["arn:aws:s3:::dw-user3-ecommerce-20261027", "arn:aws:s3:::dw-user3-ecommerce-20261027/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
}

# ------------------------------------------------- Rol de Redshift para COPY (3.3)
resource "aws_iam_role" "redshift_s3" {
  name        = "dw-user3-redshift-s3-role"
  path        = "/"
  description = "Permite a Redshift leer el bucket del taller DW con COPY"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "redshift.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
  max_session_duration  = 3600
  force_detach_policies = false
}

resource "aws_iam_role_policy" "redshift_s3" {
  name = "dw-user3-leer-raw-s3"
  role = aws_iam_role.redshift_s3.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListarBucketDelTaller"
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = "arn:aws:s3:::dw-user3-ecommerce-20261027"
      },
      {
        Sid      = "LeerArchivosRaw"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = "arn:aws:s3:::dw-user3-ecommerce-20261027/raw/*"
      },
    ]
  })
}

# ------------------------------------------------- Redshift Serverless (3.3)
resource "aws_redshiftserverless_namespace" "dw" {
  namespace_name       = "dw-user3-namespace"
  db_name              = "dev"
  default_iam_role_arn = aws_iam_role.redshift_s3.arn
  iam_roles            = [aws_iam_role.redshift_s3.arn]
  log_exports          = []

  lifecycle {
    ignore_changes = [kms_key_id, iam_roles, admin_user_password, manage_admin_password]
  }
}

resource "aws_redshiftserverless_workgroup" "dw" {
  workgroup_name       = "dw-user3-workgroup"
  namespace_name       = aws_redshiftserverless_namespace.dw.namespace_name
  base_capacity        = 4 # capacidad mínima
  max_capacity         = 0
  port                 = 5439
  publicly_accessible  = false
  enhanced_vpc_routing = false
  track_name           = "current"
  # SG por defecto de la VPC + SG propio que solo admite 5439 desde la EC2 (redshift_access.tf)
  security_group_ids = ["sg-069f0ebee63cc107a", aws_security_group.redshift.id]
  subnet_ids         = data.aws_subnets.default.ids

  lifecycle {
    ignore_changes = [config_parameter, price_performance_target]
  }
}
