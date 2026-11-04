variable "region" {
  type    = string
  default = "us-east-1"
}

variable "bucket" {
  type        = string
  default     = "data-lake-sct"
  description = "Bucket existente del data lake (no lo gestiona Terraform)."
}

variable "database_name" {
  type    = string
  default = "datalake_news"
}

variable "workgroup_name" {
  type    = string
  default = "datalake-wg"
}

variable "scan_limit_bytes" {
  type        = number
  default     = 104857600 # 100 MB por consulta: protección de costos
  description = "Corta cualquier consulta que escanee más de este valor."
}