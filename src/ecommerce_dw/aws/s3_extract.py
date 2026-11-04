#!/usr/bin/env python3
"""Extracción por lote: RDS MySQL y DocumentDB -> S3 (CSV gzip) -> registro en audit.batch_log.

Equivale a extract_mysql/extract_mongo de la versión local, pero:
  * escribe en S3 en raw/<fuente>/<entidad>/batch_id=<id>/<entidad>.csv.gz;
  * agrega a cada fila las columnas de control _batch_id, _extracted_at y _source;
  * nunca sobrescribe en silencio un lote completo (escritura condicional de S3);
  * deja un manifiesto por lote en control/batches/batch_id=<id>/manifest.json;
  * registra cada archivo en audit.batch_log con estado 'extracted'.
La marca de avance (audit.extraction_watermark) NO se mueve aquí: la mueve la
carga a Redshift cuando el lote queda cargado, para no saltar registros si algo falla.

Estrategia por entidad
  customers, products  -> extracción COMPLETA en cada lote. Son catálogos pequeños y
                          sus cambios llevan updated_at retroactivo (simulate_changes usa
                          2025-07-01), así que un filtro "updated_at > último" los perdería.
                          dbt snapshot detecta qué cambió y versiona (SCD2).
  orders, order_items, reviews, navigation_events
                       -> INCREMENTAL por llave numérica creciente (id > último extraído).
                          Los lotes nuevos usan ids >= 1.000.000 (append_batch.py).
  Limitación documentada: el incremental por id no detecta borrados ni ediciones
  de filas ya extraídas en los hechos.

Uso
  python -m ecommerce_dw.aws.s3_extract --batch-id 0 --mode initial
  python -m ecommerce_dw.aws.s3_extract --batch-id 3 --mode incremental
  python -m ecommerce_dw.aws.s3_extract --batch-id 0 --mode initial --local-dir ./salida   # prueba sin AWS
"""

import argparse
import csv
import gzip
import json
import logging
import os
import tempfile
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

from ecommerce_dw.ingestion.raw_sources import BATCH_SIZE, connect_mysql, flatten_mongo

# Mismas columnas que la versión local (raw_ingestion.py), copiadas aquí para no depender de DuckDB.
MYSQL_CSV_COLUMNS = {
    "customers": ["customer_id", "name", "email", "phone", "city", "department", "registration_date", "updated_at"],
    "products": ["product_id", "name", "category", "price", "description", "is_active", "updated_at"],
    "orders": ["order_id", "customer_id", "order_date", "status", "sales_channel", "payment_method", "total"],
    "order_items": ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "subtotal"],
}
MONGO_CSV_COLUMNS = {
    "reviews": ["review_id", "customer_id", "product_id", "rating", "reviewed_at", "document_json"],
    "navigation_events": ["event_id", "event_type", "occurred_at", "customer_id", "product_id", "sales_channel", "document_json"],
}


def _csv_value(value):
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value

log = logging.getLogger("s3_extract")

CONTROL_COLUMNS = ["_batch_id", "_extracted_at", "_source"]

# (fuente, entidad, llave para ordenar, columna de marca de avance o None = completa)
ENTITIES = [
    ("mysql", "customers", "customer_id", None),
    ("mysql", "products", "product_id", None),
    ("mysql", "orders", "order_id", "order_id"),
    ("mysql", "order_items", "order_item_id", "order_item_id"),
    ("mongodb", "reviews", "review_id", "review_id"),
    ("mongodb", "navigation_events", "event_id", "event_id"),
]


class BatchAlreadyExistsError(RuntimeError):
    """El lote ya está completo en S3: no se sobrescribe."""


def s3_data_key(source, entity, batch_id):
    return f"raw/{source}/{entity}/batch_id={batch_id}/{entity}.csv.gz"


def s3_manifest_key(batch_id):
    return f"control/batches/batch_id={batch_id}/manifest.json"


# --------------------------------------------------------------- lectura de fuentes
def read_mysql(entity, order_key, watermark_column, since):
    columns = list(MYSQL_CSV_COLUMNS[entity])
    sql = f"SELECT {', '.join(columns)} FROM {entity}"
    params = ()
    if watermark_column and since is not None:
        sql += f" WHERE {watermark_column} > %s"
        params = (since,)
    sql += f" ORDER BY {order_key}"
    connection = connect_mysql()
    try:
        with closing(connection.cursor()) as cursor:
            cursor.execute(sql, params)
            while batch := cursor.fetchmany(BATCH_SIZE):
                yield from batch
    finally:
        connection.close()


def read_mongo(entity, order_key, watermark_column, since):
    from pymongo import MongoClient

    client = MongoClient(os.environ["MONGODB_URI"])
    try:
        collection = client[os.getenv("MONGO_DATABASE", "ecommerce")][entity]
        query = {}
        if watermark_column and since is not None:
            query = {watermark_column: {"$gt": since}}
        cursor = collection.find(query).sort("_id", 1).batch_size(BATCH_SIZE)
        for document in cursor:
            _, values = flatten_mongo(entity, document, None)
            row = list(values[:-2])  # sin _loaded_at ni _source de la versión local
            # Las fechas de MongoDB/DocumentDB vienen en UTC sin zona: se marca explícitamente.
            yield [v.replace(tzinfo=timezone.utc) if isinstance(v, datetime) and v.tzinfo is None else v
                   for v in row]
    finally:
        client.close()


# ------------------------------------------------------------ escritura del archivo
def write_gzip_csv(path, headers, rows, control_values, watermark_index):
    """Escribe el CSV comprimido y devuelve (filas escritas, mayor valor de la marca)."""
    count, max_mark = 0, None
    with gzip.open(path, "wt", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(headers + CONTROL_COLUMNS)
        for row in rows:
            writer.writerow([_csv_value(v) for v in row] + control_values)
            count += 1
            if watermark_index is not None and row[watermark_index] is not None:
                mark = row[watermark_index]
                max_mark = mark if max_mark is None else max(max_mark, mark)
    return count, max_mark


# --------------------------------------------------------- destinos: S3 o carpeta local
class S3Target:
    def __init__(self, bucket):
        import boto3

        self.bucket = bucket
        self.s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-2"))

    def exists(self, key):
        from botocore.exceptions import ClientError

        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError as error:
            if error.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                return False
            raise

    def has_partial_files(self, batch_id):
        for source, entity, _, _ in ENTITIES:
            if self.exists(s3_data_key(source, entity, batch_id)):
                return True
        return False

    def put_file(self, local_path, key, allow_replace):
        from botocore.exceptions import ClientError

        extra = {} if allow_replace else {"IfNoneMatch": "*"}  # escritura condicional: falla si ya existe
        try:
            with open(local_path, "rb") as body:
                self.s3.put_object(Bucket=self.bucket, Key=key, Body=body,
                                   ContentType="text/csv", ContentEncoding="gzip", **extra)
        except ClientError as error:
            if error.response["Error"]["Code"] in ("PreconditionFailed", "ConditionalRequestConflict"):
                raise BatchAlreadyExistsError(f"s3://{self.bucket}/{key} ya existe; no se sobrescribe") from error
            raise

    def put_json(self, key, payload):
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=json.dumps(payload, indent=2).encode(),
                           ContentType="application/json", IfNoneMatch="*")

    def uri(self, key):
        return f"s3://{self.bucket}/{key}"


class LocalTarget:
    """Mismo contrato que S3Target pero en una carpeta: sirve para probar sin AWS."""

    def __init__(self, root):
        self.root = Path(root)

    def exists(self, key):
        return (self.root / key).exists()

    def has_partial_files(self, batch_id):
        return any(self.exists(s3_data_key(s, e, batch_id)) for s, e, _, _ in ENTITIES)

    def put_file(self, local_path, key, allow_replace):
        destination = self.root / key
        if destination.exists() and not allow_replace:
            raise BatchAlreadyExistsError(f"{destination} ya existe; no se sobrescribe")
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(Path(local_path).read_bytes())

    def put_json(self, key, payload):
        destination = self.root / key
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def uri(self, key):
        return str(self.root / key)


# ------------------------------------------------------------------ marcas de avance
def read_watermarks_redshift():
    from ecommerce_dw.aws import redshift_data

    rows = redshift_data.query(
        "select source, entity, last_value from audit.extraction_watermark")
    return {(r["source"], r["entity"]): r["last_value"] for r in rows}


def read_watermarks_local(target, batch_id):
    """En modo local, la marca es la del manifiesto del lote anterior más reciente."""
    manifests = sorted(target.root.glob("control/batches/batch_id=*/manifest.json"),
                       key=lambda p: int(p.parent.name.split("=")[1]))
    marks = {}
    for path in manifests:
        if int(path.parent.name.split("=")[1]) >= batch_id:
            continue
        for item in json.loads(path.read_text(encoding="utf-8"))["files"]:
            if item["watermark_to"] is not None:
                marks[(item["source"], item["entity"])] = item["watermark_to"]
    return marks


def register_in_audit(manifest):
    from ecommerce_dw.aws import redshift_data as rd

    batch_id = manifest["batch_id"]
    statements = [f"delete from audit.batch_log where batch_id = {int(batch_id)}"]
    for item in manifest["files"]:
        statements.append(
            "insert into audit.batch_log (batch_id, source, entity, s3_key, record_count, "
            "extracted_at, status, dag_run_id) values ("
            + ", ".join(rd.sql_literal(v) for v in (
                batch_id, item["source"], item["entity"], item["s3_key"], item["record_count"],
                manifest["extracted_at"], "extracted", manifest.get("run_id")))
            + ")")
    rd.execute_transaction(statements)


# ------------------------------------------------------------------------ principal
def extract_batch(batch_id, mode, target, run_id=None, watermarks=None):
    if mode not in ("initial", "incremental"):
        raise ValueError("mode debe ser 'initial' o 'incremental'")
    manifest_key = s3_manifest_key(batch_id)
    if target.exists(manifest_key):
        raise BatchAlreadyExistsError(
            f"El lote {batch_id} ya está completo ({target.uri(manifest_key)}). Para reprocesarlo "
            "se recarga desde S3 sin extraer de nuevo; para datos nuevos use otro batch_id.")
    allow_replace = target.has_partial_files(batch_id)
    if allow_replace:
        log.warning("Lote %s incompleto de una ejecución fallida (sin manifiesto): se reemplazan sus "
                    "archivos. El versionado del bucket conserva las versiones anteriores.", batch_id)

    watermarks = watermarks or {}
    extracted_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00")
    files = []
    with tempfile.TemporaryDirectory() as workdir:
        for source, entity, order_key, watermark_column in ENTITIES:
            since = None
            if mode == "incremental" and watermark_column:
                if (source, entity) not in watermarks:
                    raise RuntimeError(f"No hay marca de avance para {source}.{entity}: "
                                       "ejecute primero la carga inicial (lote 0) y su carga a Redshift.")
                since = int(watermarks[(source, entity)])
            if source == "mysql":
                headers, rows = list(MYSQL_CSV_COLUMNS[entity]), read_mysql(entity, order_key, watermark_column, since)
            else:
                headers, rows = list(MONGO_CSV_COLUMNS[entity]), read_mongo(entity, order_key, watermark_column, since)
            watermark_index = headers.index(watermark_column) if watermark_column else None
            local_path = Path(workdir) / f"{entity}.csv.gz"
            control = [batch_id, extracted_at, f"{source}.{entity}"]
            count, max_mark = write_gzip_csv(local_path, headers, rows, control, watermark_index)
            key = s3_data_key(source, entity, batch_id)
            target.put_file(local_path, key, allow_replace)
            watermark_to = max_mark if max_mark is not None else since
            files.append({
                "source": source, "entity": entity, "s3_key": key, "record_count": count,
                "strategy": "incremental" if watermark_column else "full",
                "watermark_column": watermark_column, "watermark_from": since,
                "watermark_to": watermark_to,
            })
            log.info("batch_id=%s %s.%s: %s filas -> %s", batch_id, source, entity, count, target.uri(key))

    manifest = {"batch_id": batch_id, "mode": mode, "extracted_at": extracted_at,
                "run_id": run_id, "files": files}
    target.put_json(manifest_key, manifest)  # el manifiesto se escribe AL FINAL: marca el lote como completo
    log.info("batch_id=%s manifiesto -> %s", batch_id, target.uri(manifest_key))
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--batch-id", type=int, required=True, help="0 = carga inicial; 1..365 = lotes incrementales")
    parser.add_argument("--mode", choices=["initial", "incremental"], required=True)
    parser.add_argument("--run-id", default=os.getenv("AIRFLOW_CTX_DAG_RUN_ID"))
    parser.add_argument("--local-dir", help="Escribe en esta carpeta en vez de S3 y no toca Redshift (prueba)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    if args.local_dir:
        target = LocalTarget(args.local_dir)
        watermarks = read_watermarks_local(target, args.batch_id) if args.mode == "incremental" else {}
        extract_batch(args.batch_id, args.mode, target, args.run_id, watermarks)
        return
    target = S3Target(os.environ["S3_BUCKET"])
    watermarks = read_watermarks_redshift() if args.mode == "incremental" else {}
    manifest = extract_batch(args.batch_id, args.mode, target, args.run_id, watermarks)
    register_in_audit(manifest)
    log.info("batch_id=%s registrado en audit.batch_log con estado 'extracted'", args.batch_id)


if __name__ == "__main__":
    main()
