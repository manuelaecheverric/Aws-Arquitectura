#!/usr/bin/env bash
# Uso (en la EC2):  source scripts/aws/load_env.sh
# Lee ~/.dw-aws.conf (endpoints y ARNs, sin contraseñas) y obtiene las contraseñas de
# Secrets Manager usando el rol de la EC2. Las claves solo viven en la memoria de la sesión.
set -a
source "$HOME/.dw-aws.conf"
_secret() { aws secretsmanager get-secret-value --region "$AWS_REGION" --secret-id "$1" --query SecretString --output text; }
_field() { python3 -c 'import json,sys; print(json.loads(sys.stdin.read())[sys.argv[1]])' "$1"; }
_mysql_json="$(_secret "$MYSQL_SECRET_ARN")"
MYSQL_USER="$(printf '%s' "$_mysql_json" | _field username)"
MYSQL_PASSWORD="$(printf '%s' "$_mysql_json" | _field password)"
_docdb_json="$(_secret "$DOCDB_SECRET_ARN")"
_docdb_user="$(printf '%s' "$_docdb_json" | _field username)"
_docdb_pass="$(printf '%s' "$_docdb_json" | _field password)"
MONGODB_URI="mongodb://${_docdb_user}:${_docdb_pass}@${DOCDB_HOST}:27017/?tls=true&tlsCAFile=/opt/certs/global-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"
_rs_json="$(_secret "$REDSHIFT_SECRET_ARN")"
REDSHIFT_USER="$(printf '%s' "$_rs_json" | _field username)"
REDSHIFT_PASSWORD="$(printf '%s' "$_rs_json" | _field password)"
REDSHIFT_HOST="$(aws redshift-serverless get-workgroup --region "$AWS_REGION" --workgroup-name "$REDSHIFT_WORKGROUP" --query 'workgroup.endpoint.address' --output text)"
DBT_TARGET=aws
DBT_PROFILES_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/src/dbt"
MYSQL_PORT=3306
MYSQL_DATABASE=ecommerce
MONGO_DATABASE=ecommerce
PYTHONPATH="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/src"
unset _mysql_json _docdb_json _docdb_pass _rs_json
set +a
echo "Entorno cargado: MySQL=$MYSQL_HOST DocumentDB=$DOCDB_HOST Redshift=$REDSHIFT_HOST Bucket=$S3_BUCKET"
