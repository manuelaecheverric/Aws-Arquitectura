"""Ejecuta SQL en Redshift Serverless con la Redshift Data API.

Por qué la Data API y no un driver (psycopg2/redshift_connector):
  * No abre conexiones de red al puerto 5439: llama a un endpoint HTTPS de AWS,
    así Redshift puede seguir SIN acceso público.
  * La contraseña nunca aparece en el código: se pasa el ARN del secreto de
    Secrets Manager y AWS la resuelve con los permisos IAM de quien ejecuta.

Variables de entorno:
  AWS_REGION            (por defecto us-east-2)
  REDSHIFT_WORKGROUP    p. ej. dw-user3-workgroup
  REDSHIFT_DATABASE     (por defecto dev)
  REDSHIFT_SECRET_ARN   ARN del secreto redshift!dw-user3-namespace-dwadmin
"""

import os
import time

import boto3


class RedshiftDataError(RuntimeError):
    """La sentencia terminó en FAILED o ABORTED."""


def _client():
    return boto3.client("redshift-data", region_name=os.getenv("AWS_REGION", "us-east-2"))


def _target():
    return {
        "WorkgroupName": os.environ["REDSHIFT_WORKGROUP"],
        "Database": os.getenv("REDSHIFT_DATABASE", "dev"),
        "SecretArn": os.environ["REDSHIFT_SECRET_ARN"],
    }


def sql_literal(value):
    """Convierte un valor de Python en literal SQL seguro (para batch_execute_statement,
    que no admite parámetros)."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"


def _wait(client, statement_id, timeout_seconds):
    deadline = time.monotonic() + timeout_seconds
    while True:
        description = client.describe_statement(Id=statement_id)
        status = description["Status"]
        if status == "FINISHED":
            return description
        if status in ("FAILED", "ABORTED"):
            sql = (description.get("QueryString") or "")[:300]
            raise RedshiftDataError(f"{status}: {description.get('Error')} | SQL: {sql}")
        if time.monotonic() > deadline:
            client.cancel_statement(Id=statement_id)
            raise TimeoutError(f"La sentencia {statement_id} superó {timeout_seconds} s")
        time.sleep(1)


def execute(sql, parameters=None, timeout_seconds=900):
    """Ejecuta una sentencia. `parameters` usa la sintaxis :nombre de la Data API."""
    client = _client()
    request = {**_target(), "Sql": sql}
    if parameters:
        request["Parameters"] = [{"name": k, "value": str(v)} for k, v in parameters.items()]
    statement_id = client.execute_statement(**request)["Id"]
    return _wait(client, statement_id, timeout_seconds)


def execute_transaction(statements, timeout_seconds=900):
    """Ejecuta varias sentencias en UNA transacción: se aplican todas o ninguna."""
    client = _client()
    statement_id = client.batch_execute_statement(Sqls=list(statements), **_target())["Id"]
    return _wait(client, statement_id, timeout_seconds)


def _field_value(field):
    if field.get("isNull"):
        return None
    for key in ("stringValue", "longValue", "doubleValue", "booleanValue"):
        if key in field:
            return field[key]
    return None


def query(sql, parameters=None):
    """Ejecuta un SELECT y devuelve una lista de diccionarios {columna: valor}."""
    description = execute(sql, parameters)
    if not description.get("HasResultSet"):
        return []
    client = _client()
    rows, columns, token = [], None, None
    while True:
        request = {"Id": description["Id"]}
        if token:
            request["NextToken"] = token
        page = client.get_statement_result(**request)
        columns = columns or [meta["name"] for meta in page["ColumnMetadata"]]
        rows.extend({c: _field_value(f) for c, f in zip(columns, record)} for record in page["Records"])
        token = page.get("NextToken")
        if not token:
            return rows
