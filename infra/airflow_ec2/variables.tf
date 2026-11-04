variable "region" {
  type    = string
  default = "us-east-1"
}

variable "vpc_id" {
  type    = string
  default = "vpc-0800daa55eae67d34"
}

variable "bucket_name" {
  type    = string
  default = "data-lake-sct"
}

variable "key_name" {
  description = "Nombre del key pair creado en la consola AWS"
  type        = string
  default     = "de-gr01-airflow-key"
}

variable "my_ip" {
  description = "Tu IP publica (sin /32). Cambiala si tu IP cambia."
  type        = string
  default     = "186.80.31.18"
}

variable "instance_type" {
  type    = string
  default = "t3.medium"
}

variable "disk_gb" {
  type    = number
  default = 20
}
