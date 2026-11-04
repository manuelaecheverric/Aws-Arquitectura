# EC2 donde corren los scripts, Airflow y dbt. Obtiene permisos por su ROL (perfil de
# instancia): no hay llaves de AWS guardadas en el servidor.

data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]
  filter {
    name   = "name"
    values = ["al2023-ami-2023*-x86_64"]
  }
}

data "aws_caller_identity" "current" {}

resource "aws_iam_role" "ec2" {
  name = "${var.prefix}-ec2-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "ec2" {
  name = "${var.prefix}-ec2-pipeline"
  role = aws_iam_role.ec2.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "BucketDelTaller"
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = "arn:aws:s3:::${var.bucket_name}"
      },
      {
        Sid      = "ObjetosDelTaller"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject"]
        Resource = "arn:aws:s3:::${var.bucket_name}/*"
      },
      {
        Sid    = "SecretosDelTaller"
        Effect = "Allow"
        Action = ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
        Resource = [
          aws_secretsmanager_secret.docdb.arn,
          aws_db_instance.mysql.master_user_secret[0].secret_arn,
          var.redshift_secret_arn,
        ]
      },
      {
        Sid      = "RedshiftDataAPI"
        Effect   = "Allow"
        Action   = ["redshift-data:ExecuteStatement", "redshift-data:BatchExecuteStatement", "redshift-data:DescribeStatement", "redshift-data:GetStatementResult", "redshift-data:CancelStatement", "redshift-data:ListStatements"]
        Resource = "*"
      },
      {
        Sid      = "RedshiftServerlessCredenciales"
        Effect   = "Allow"
        Action   = ["redshift-serverless:GetCredentials", "redshift-serverless:GetWorkgroup"]
        Resource = "*"
      },
    ]
  })
}

resource "aws_iam_instance_profile" "ec2" {
  name = "${var.prefix}-ec2-profile"
  role = aws_iam_role.ec2.name
}

resource "aws_instance" "pipeline" {
  ami                         = data.aws_ami.al2023.id
  instance_type               = var.ec2_instance_type
  subnet_id                   = data.aws_subnets.default.ids[0]
  vpc_security_group_ids      = [aws_security_group.ec2.id]
  iam_instance_profile        = aws_iam_instance_profile.ec2.name
  associate_public_ip_address = true # solo para SALIR a internet (paquetes); no hay entrada pública

  root_block_device {
    volume_size = 30
    volume_type = "gp3"
    encrypted   = true
  }

  metadata_options {
    http_tokens                 = "required" # IMDSv2
    http_put_response_hop_limit = 2          # los contenedores de Airflow (Docker) necesitan 1 salto extra para usar el rol
  }

  user_data = <<-EOT
    #!/bin/bash
    set -eux
    dnf install -y python3.11 python3.11-pip mariadb105 git docker
    mkdir -p /usr/local/lib/docker/cli-plugins
    curl -sSfL https://github.com/docker/compose/releases/download/v2.29.7/docker-compose-linux-x86_64 -o /usr/local/lib/docker/cli-plugins/docker-compose
    chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
    systemctl enable --now docker
    usermod -aG docker ec2-user
    mkdir -p /opt/certs
    curl -sSfo /opt/certs/global-bundle.pem https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem
    chmod 644 /opt/certs/global-bundle.pem
  EOT

  tags = { Name = "${var.prefix}-pipeline" }

  # user_data solo corre al crear la instancia; cambiarlo después no debe reemplazarla.
  lifecycle {
    ignore_changes = [user_data, associate_public_ip_address] # la IP pública desaparece al detenerla
  }
}
