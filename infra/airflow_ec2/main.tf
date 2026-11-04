terraform {
  required_version = ">= 1.3"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      Project = "datalake-news"
      Owner   = "de-gr01-user-4"
      Phase   = "airflow-ec2"
    }
  }
}

# ---------- AMI Ubuntu 24.04 ----------
data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
  }
}

# ---------- Subnet de la VPC por defecto ----------
data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [var.vpc_id]
  }
  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

# ---------- Security group: solo SSH desde tu IP ----------
resource "aws_security_group" "airflow" {
  name        = "datalake-airflow-sg"
  description = "SSH solo desde mi IP; Airflow 8080 NO expuesto (tunel SSH)"
  vpc_id      = var.vpc_id

  ingress {
    description = "SSH desde mi IP"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["${var.my_ip}/32"]
  }

  egress {
    description = "Salida libre (apt, Docker Hub, APIs AWS, RSS)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ---------- Rol IAM propio + perfil de instancia ----------
data "aws_iam_policy_document" "assume_ec2" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "airflow" {
  name               = "datalake-airflow-ec2-role"
  assume_role_policy = data.aws_iam_policy_document.assume_ec2.json
}

data "aws_iam_policy_document" "permisos" {
  statement {
    sid       = "S3ListBucket"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = ["arn:aws:s3:::${var.bucket_name}"]
  }
  statement {
    sid       = "S3Objetos"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    resources = ["arn:aws:s3:::${var.bucket_name}/*"]
  }
  statement {
    sid = "Comprehend"
    actions = [
      "comprehend:DetectEntities", "comprehend:DetectKeyPhrases",
      "comprehend:BatchDetectEntities", "comprehend:BatchDetectKeyPhrases"
    ]
    resources = ["*"]
  }
  statement {
    sid = "Glue"
    actions = [
      "glue:GetDatabase", "glue:GetDatabases",
      "glue:GetTable", "glue:GetTables",
      "glue:GetPartition", "glue:GetPartitions",
      "glue:BatchCreatePartition", "glue:CreatePartition",
      "glue:UpdatePartition", "glue:BatchGetPartition"
    ]
    resources = ["*"]
  }
  statement {
    sid = "Athena"
    actions = [
      "athena:StartQueryExecution", "athena:GetQueryExecution",
      "athena:GetQueryResults", "athena:StopQueryExecution",
      "athena:GetWorkGroup", "athena:ListWorkGroups"
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "airflow" {
  name   = "datalake-airflow-permisos"
  role   = aws_iam_role.airflow.id
  policy = data.aws_iam_policy_document.permisos.json
}

resource "aws_iam_instance_profile" "airflow" {
  name = "datalake-airflow-ec2-profile"
  role = aws_iam_role.airflow.name
}

# ---------- EC2 ----------
resource "aws_instance" "airflow" {
  ami                         = data.aws_ami.ubuntu.id
  instance_type               = var.instance_type
  subnet_id                   = data.aws_subnets.default.ids[0]
  vpc_security_group_ids      = [aws_security_group.airflow.id]
  key_name                    = var.key_name
  iam_instance_profile        = aws_iam_instance_profile.airflow.name
  associate_public_ip_address = true
  user_data                   = file("${path.module}/user_data.sh")

  # hop_limit=2: permite que los contenedores Docker obtengan las
  # credenciales del rol (sin claves en la maquina).
  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
  }

  root_block_device {
    volume_type           = "gp3"
    volume_size           = var.disk_gb
    delete_on_termination = true
  }

  tags = {
    Name = "datalake-airflow"
  }
}
