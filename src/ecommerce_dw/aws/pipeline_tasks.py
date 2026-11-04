"""Funciones que ejecutan las tareas del DAG dw_aws_pipeline (versión AWS).

Cada función registra en sus logs el batch_id y la tarea, para identificar qué lote y qué
paso fallaron (requisito 3.4). El estado global de cada ejecución queda en
audit.pipeline_runs; el de cada archivo del lote, en audit.batch_log.
"""

import logging
import os
import subprocess
from datetime import datetime, timezone

import boto3

from ecommerce_dw.aws import redshift_data as rd
from ecommerce_dw.aws import secrets
from ecommerce_dw.aws.s3_extract import S3Target, extract_batch, read_watermarks_redshift, s3_manifest_key
from ecommerce_dw.aws.s3_load import load_batch

LOGGER = logging.getLogger("dw_aws_pipeline")
DBT_EXECUTABLE = os.getenv("DBT_EXECUTABLE", "/home/airflow/dbt-venv/bin/dbt")
DBT_PROJECT = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/src/dbt")


def _log(batch_id, task, message, *args):
    LOGGER.info("[batch_id=%s][task=%s] " + message, batch_id, task, *args)


def resolve_batch(params, run_id):
    """Decide lote y modo. batch_id < 0 = siguiente lote libre según audit.batch_log."""
    batch_id = int(params.get("batch_id", -1))
    mode = params.get("mode", "incremental")
    if batch_id < 0:
        rows = rd.query("select coalesce(max(batch_id), -1) as last from audit.batch_log")
        batch_id = int(rows[0]["last"]) + 1
        mode = "initial" if batch_id == 0 else "incremental"
    result = {"batch_id": batch_id, "mode": mode, "reprocess": bool(params.get("reprocess", False)),
              "fail_task": params.get("fail_task") or ""}
    _log(batch_id, "resolve_batch", "lote=%s modo=%s reprocesar=%s run_id=%s", batch_id, mode, result["reprocess"], run_id)
    return result


def maybe_fail(batch, task_name):
    """Escenario 3.5 "fallo controlado": falla a propósito la tarea indicada en el parámetro fail_task."""
    if batch.get("fail_task") == task_name:
        _log(batch["batch_id"], task_name, "FALLO CONTROLADO solicitado por el parámetro fail_task")
        raise RuntimeError(f"Fallo controlado en {task_name} (batch_id={batch['batch_id']})")


def validate_connections(batch):
    """3.4.1: parámetros y conexiones antes de mover datos."""
    b = batch["batch_id"]
    for name in ("S3_BUCKET", "REDSHIFT_WORKGROUP", "REDSHIFT_SECRET_ARN"):
        if not os.getenv(name):
            raise ValueError(f"Falta la variable {name}")
    boto3.client("s3", region_name=secrets.REGION).head_bucket(Bucket=os.environ["S3_BUCKET"])
    _log(b, "validate", "S3 OK")
    rd.query("select 1 as ok")
    _log(b, "validate", "Redshift (Data API) OK")
    if batch["reprocess"]:
        _log(b, "validate", "reproceso: no se consultan las fuentes")
        return
    secrets.apply(secrets.source_env())
    from ecommerce_dw.ingestion.raw_sources import connect_mysql
    from pymongo import MongoClient

    connection = connect_mysql(); connection.ping(); connection.close()
    _log(b, "validate", "RDS MySQL OK")
    client = MongoClient(os.environ["MONGODB_URI"], serverSelectionTimeoutMS=10000)
    client.admin.command("ping"); client.close()
    _log(b, "validate", "DocumentDB OK")


def register_start(batch, run_id):
    rd.execute_transaction([
        f"delete from audit.pipeline_runs where dag_run_id = {rd.sql_literal(run_id)}",
        "insert into audit.pipeline_runs (dag_run_id, batch_id, started_at, status) values ("
        f"{rd.sql_literal(run_id)}, {int(batch['batch_id'])}, getdate(), 'running')",
    ])
    _log(batch["batch_id"], "register_start", "ejecución registrada como running")


def extract(batch, run_id):
    """3.4.2-3: fuentes -> S3. En reproceso no extrae: exige que el manifiesto exista."""
    b = batch["batch_id"]
    target = S3Target(os.environ["S3_BUCKET"])
    if batch["reprocess"]:
        if not target.exists(s3_manifest_key(b)):
            raise RuntimeError(f"No existe el lote {b} en S3; no se puede reprocesar")
        _log(b, "extract", "reproceso: se reutilizan los archivos del lote en S3")
        return
    secrets.apply(secrets.source_env())
    watermarks = read_watermarks_redshift() if batch["mode"] == "incremental" else {}
    manifest = extract_batch(b, batch["mode"], target, run_id, watermarks)
    from ecommerce_dw.aws.s3_extract import register_in_audit

    register_in_audit(manifest)
    _log(b, "extract", "archivos: %s", {f["entity"]: f["record_count"] for f in manifest["files"]})


def load(batch, run_id):
    """3.4.4: S3 -> raw con COPY (idempotente)."""
    maybe_fail(batch, "load_to_redshift")
    results = load_batch(batch["batch_id"], os.environ["S3_BUCKET"], run_id)
    _log(batch["batch_id"], "load", "raw: %s", {r["entity"]: r["loaded_count"] for r in results})


def run_dbt(batch, command, selector=None):
    """3.4.5-6: transformaciones, snapshots y pruebas de dbt; el SQL se ejecuta en Redshift."""
    maybe_fail(batch, f"dbt_{command}")
    env = {**os.environ, **secrets.dbt_env()}
    arguments = [DBT_EXECUTABLE, command, "--project-dir", DBT_PROJECT, "--profiles-dir", DBT_PROJECT]
    if selector:
        arguments += ["--select", selector]
    _log(batch["batch_id"], f"dbt_{command}", "%s", " ".join(arguments))
    completed = subprocess.run(arguments, env=env, capture_output=True, text=True)
    for line in completed.stdout.splitlines():
        LOGGER.info("[batch_id=%s][dbt] %s", batch["batch_id"], line)
    if completed.returncode != 0:
        raise RuntimeError(f"dbt {command} falló (batch_id={batch['batch_id']}); ver log de la tarea")


def register_result(batch, run_id, task_states):
    """3.4.7: resultado final. Se ejecuta siempre (trigger_rule=all_done)."""
    failed = [task for task, state in task_states.items() if state in ("failed", "upstream_failed")]
    status = "failed" if failed else "success"
    first_failed = next((t for t, s in task_states.items() if s == "failed"), None)
    rd.execute(
        "update audit.pipeline_runs set finished_at = getdate(), status = "
        f"{rd.sql_literal(status)}, failed_task = {rd.sql_literal(first_failed)}, message = "
        f"{rd.sql_literal('tareas con error: ' + ', '.join(failed) if failed else 'ok')} "
        f"where dag_run_id = {rd.sql_literal(run_id)}"
    )
    _log(batch["batch_id"] if batch else None, "register_result", "estado=%s tarea_fallida=%s", status, first_failed)
    if failed:
        raise RuntimeError(f"La ejecución terminó con fallos en: {failed}")
