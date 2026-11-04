terraform {
  required_version = ">= 1.5"
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
      project    = "data-lake-news"
      course     = "ingenieria-de-datos"
      managed_by = "terraform"
    }
  }
}

locals {
  # Formato de cada columna: "nombre:tipo". pub_date NO va aquí: es clave de partición.
  tables = {
    silver_news = {
      path = "silver/news/"
      columns = [
        "news_id:string", "guid:string", "link:string", "title:string",
        "description:string", "pub_date_original:string",
        "published_at_utc:timestamp", "medio:string", "seccion_principal:string",
        "secciones:array<string>", "content_hash:string",
        "first_seen_run_id:string", "last_seen_run_id:string",
        "updated_run_id:string", "bronze_run_id:string", "bronze_s3_uri:string",
        "ingested_at_utc:timestamp", "analyzed_text:string", "text_hash:string",
        "language_code:string", "analyzed_at_utc:timestamp",
        "enrichment_status:string",
      ]
    }
    silver_news_entities = {
      path = "silver/news_entities/"
      columns = [
        "news_id:string", "entity_text:string", "entity_type:string",
        "score:double", "text_hash:string",
      ]
    }
    silver_news_key_phrases = {
      path = "silver/news_key_phrases/"
      columns = [
        "news_id:string", "key_phrase:string", "score:double", "text_hash:string",
      ]
    }
    gold_news_daily = {
      path    = "gold/news_daily/"
      columns = ["medio:string", "seccion_principal:string", "news_count:bigint"]
    }
    gold_entity_daily = {
      path    = "gold/entity_daily/"
      columns = ["entity_type:string", "entity_text:string", "news_count:bigint"]
    }
    gold_key_phrase_section_daily = {
      path    = "gold/key_phrase_section_daily/"
      columns = ["seccion_principal:string", "key_phrase:string", "news_count:bigint"]
    }
  }
}

resource "aws_glue_catalog_database" "this" {
  name        = var.database_name
  description = "Data lake de noticias RSS (capas Silver y Gold)."
}

resource "aws_glue_catalog_table" "t" {
  for_each      = local.tables
  name          = each.key
  database_name = aws_glue_catalog_database.this.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL              = "TRUE"
    classification        = "parquet"
    "parquet.compression" = "SNAPPY"
  }

  partition_keys {
    name    = "pub_date"
    type    = "string"
    comment = "AAAA-MM-DD o 'unknown'. Solo existe en la ruta S3."
  }

  storage_descriptor {
    location      = "s3://${var.bucket}/${each.value.path}"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = each.key
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters = {
        "serialization.format" = "1"
      }
    }

    dynamic "columns" {
      for_each = each.value.columns
      content {
        name = split(":", columns.value)[0]
        type = split(":", columns.value)[1]
      }
    }
  }
}

resource "aws_athena_workgroup" "this" {
  name          = var.workgroup_name
  description   = "Workgroup del data lake: resultados en el mismo bucket y tope de escaneo."
  force_destroy = true # permite eliminarlo aunque tenga historial de consultas

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = false
    bytes_scanned_cutoff_per_query     = var.scan_limit_bytes

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      output_location = "s3://${var.bucket}/athena_results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}