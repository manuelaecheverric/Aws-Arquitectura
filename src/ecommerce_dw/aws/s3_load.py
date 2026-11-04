#!/usr/bin/env python3
"""Carga un lote de S3 a las tablas "raw" de Redshift con COPY, de forma idempotente.

Por cada archivo del manifiesto ejecuta UNA transacción:
  1. delete from "raw".<tabla> where _batch_id = <lote>     -> repetir el lote no duplica
  2. copy "raw".<tabla> from s3://... iam_role default       -> carga con el rol IAM
  3. actualiza audit.batch_log (loaded_count, status='loaded')
  4. mueve audit.extraction_watermark (solo entidades incrementales)
Si el COPY falla, la transacción se revierte entera: no se pierde lo cargado antes ni
avanza la marca. El lote queda en 'failed' en audit.batch_log con el error.

Se lee del manifiesto en S3, así un lote se puede reprocesar sin volver a consultar
las fuentes (requisito 3.2).

Uso:  python -m ecommerce_dw.aws.s3_load --batch-id 0
"""

import argparse
import json
import logging
import os

import boto3

from ecommerce_dw.aws import redshift_data as rd
from ecommerce_dw.aws.s3_extract import CONTROL_COLUMNS, MONGO_CSV_COLUMNS, MYSQL_CSV_COLUMNS, s3_manifest_key

log = logging.getLogger("s3_load")

TABLES = {
    ("mysql", "customers"): "mysql_customers",
    ("mysql", "products"): "mysql_products",
    ("mysql", "orders"): "mysql_orders",
    ("mysql", "order_items"): "mysql_order_items",
    ("mongodb", "reviews"): "mongo_reviews",
    ("mongodb", "navigation_events"): "mongo_navigation_events",
}


def read_manifest(bucket, batch_id):
    s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-2"))
    body = s3.get_object(Bucket=bucket, Key=s3_manifest_key(batch_id))["Body"].read()
    return json.loads(body)


def copy_statements(bucket, batch_id, item, run_id):
    table = f'"raw".{TABLES[(item["source"], item["entity"])]}'
    lit = rd.sql_literal
    where_log = (f"batch_id = {int(batch_id)} and source = {lit(item['source'])} "
                 f"and entity = {lit(item['entity'])}")
    # Lista de columnas = encabezado del CSV (datos + control). _loaded_at la pone Redshift
    # con su DEFAULT; sin esta lista COPY espera todas las columnas y falla ("Delimiter not found").
    data_columns = MYSQL_CSV_COLUMNS.get(item["entity"]) or MONGO_CSV_COLUMNS[item["entity"]]
    columns = ", ".join(list(data_columns) + CONTROL_COLUMNS)
    statements = [
        f"delete from {table} where _batch_id = {int(batch_id)}",
        f"copy {table} ({columns}) from {lit('s3://' + bucket + '/' + item['s3_key'])} "
        "iam_role default format as csv ignoreheader 1 gzip emptyasnull "
        "timeformat 'auto' dateformat 'auto' region 'us-east-2'",
        f"update audit.batch_log set loaded_count = (select count(*) from {table} "
        f"where _batch_id = {int(batch_id)}), loaded_at = getdate(), status = 'loaded', "
        f"error_message = null, dag_run_id = coalesce({lit(run_id)}, dag_run_id) where {where_log}",
    ]
    if item.get("watermark_column") and item.get("watermark_to") is not None:
        statements += [
            f"delete from audit.extraction_watermark where source = {lit(item['source'])} "
            f"and entity = {lit(item['entity'])}",
            "insert into audit.extraction_watermark (source, entity, watermark_column, last_value, "
            f"last_batch_id, updated_at) values ({lit(item['source'])}, {lit(item['entity'])}, "
            f"{lit(item['watermark_column'])}, {lit(str(item['watermark_to']))}, {int(batch_id)}, getdate())",
        ]
    return statements, where_log


def load_batch(batch_id, bucket, run_id=None):
    manifest = read_manifest(bucket, batch_id)
    results = []
    for item in manifest["files"]:
        statements, where_log = copy_statements(bucket, batch_id, item, run_id)
        try:
            rd.execute_transaction(statements)
        except Exception as error:
            rd.execute(f"update audit.batch_log set status = 'failed', error_message = "
                       f"{rd.sql_literal(str(error)[:4000])} where {where_log}")
            log.error("batch_id=%s %s.%s FALLÓ: %s", batch_id, item["source"], item["entity"], error)
            raise
        rows = rd.query(f"select loaded_count from audit.batch_log where {where_log}")
        loaded = rows[0]["loaded_count"] if rows else None
        log.info("batch_id=%s %s.%s: archivo=%s filas, raw=%s filas", batch_id, item["source"],
                 item["entity"], item["record_count"], loaded)
        if loaded != item["record_count"]:
            raise RuntimeError(f"Conteo no coincide en {item['entity']}: archivo {item['record_count']} vs raw {loaded}")
        results.append({**item, "loaded_count": loaded})
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--batch-id", type=int, required=True)
    parser.add_argument("--run-id", default=os.getenv("AIRFLOW_CTX_DAG_RUN_ID"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    load_batch(args.batch_id, os.environ["S3_BUCKET"], args.run_id)


if __name__ == "__main__":
    main()
