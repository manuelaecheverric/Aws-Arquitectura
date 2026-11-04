"""Resuelve credenciales desde AWS Secrets Manager en tiempo de ejecución.

Airflow y los scripts solo conocen ARNs (no secretos). Con el rol de la EC2 se leen las
contraseñas y se exponen como variables de entorno SOLO dentro del proceso de la tarea.
"""

import json
import os
from functools import lru_cache

import boto3

REGION = os.getenv("AWS_REGION", "us-east-2")


@lru_cache(maxsize=None)
def _secret(arn):
    client = boto3.client("secretsmanager", region_name=REGION)
    return json.loads(client.get_secret_value(SecretId=arn)["SecretString"])


@lru_cache(maxsize=1)
def redshift_host():
    client = boto3.client("redshift-serverless", region_name=REGION)
    return client.get_workgroup(workgroupName=os.environ["REDSHIFT_WORKGROUP"])["workgroup"]["endpoint"]["address"]


def source_env():
    """Variables que esperan los scripts de extracción (connect_mysql y MongoClient)."""
    mysql = _secret(os.environ["MYSQL_SECRET_ARN"])
    docdb = _secret(os.environ["DOCDB_SECRET_ARN"])
    ca_file = os.getenv("DOCDB_CA_FILE", "/opt/certs/global-bundle.pem")
    return {
        "MYSQL_USER": mysql["username"],
        "MYSQL_PASSWORD": mysql["password"],
        "MONGODB_URI": (
            f"mongodb://{docdb['username']}:{docdb['password']}@{os.environ['DOCDB_HOST']}:27017/"
            f"?tls=true&tlsCAFile={ca_file}&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"
        ),
    }


def dbt_env():
    """Variables que usa el perfil 'aws' de dbt (src/dbt/profiles.yml)."""
    redshift = _secret(os.environ["REDSHIFT_SECRET_ARN"])
    return {
        "DBT_TARGET": "aws",
        "REDSHIFT_HOST": redshift_host(),
        "REDSHIFT_USER": redshift["username"],
        "REDSHIFT_PASSWORD": redshift["password"],
    }


def apply(env):
    os.environ.update(env)
