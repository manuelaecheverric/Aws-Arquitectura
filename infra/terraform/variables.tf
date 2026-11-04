variable "region" {
  type    = string
  default = "us-east-2"
}

variable "owner" {
  description = "Usuario IAM dueño de los recursos (cuenta compartida)"
  type        = string
  default     = "de-gr01-user-3"
}

variable "prefix" {
  description = "Prefijo de nombres para no chocar con otros grupos"
  type        = string
  default     = "dw-user3"
}

variable "bucket_name" {
  description = "Bucket S3 del data warehouse (creado antes por consola)"
  type        = string
}

variable "redshift_workgroup" {
  description = "Workgroup de Redshift Serverless (creado antes por consola)"
  type        = string
  default     = "dw-user3-workgroup"
}

variable "redshift_secret_arn" {
  description = "ARN del secreto del admin de Redshift (redshift!dw-user3-namespace-dwadmin)"
  type        = string
}

variable "ssh_allowed_cidr" {
  description = "IP pública desde la que se administra la EC2 (la de CloudShell: curl checkip.amazonaws.com), en formato x.x.x.x/32"
  type        = string
}

variable "ec2_instance_type" {
  type    = string
  default = "t3.large" # Airflow + dbt necesitan ~8 GB de RAM
}

variable "rds_instance_class" {
  type    = string
  default = "db.t4g.micro"
}

variable "docdb_instance_class" {
  type    = string
  default = "db.t3.medium" # la más pequeña de DocumentDB basado en instancias
}

variable "airflow_ui_cidr" {
  description = "IP pública (x.x.x.x/32) desde la que se abre la interfaz de Airflow (puerto 8080). Vacío = cerrada."
  type        = string
  default     = ""
}
