# Despliegue, ejecución y eliminación

Región: **us-east-2**. Prefijo de nombres: `dw-user3-` (cuenta compartida del curso).
Ejecuto todo desde **AWS CloudShell** (trae la AWS CLI con las credenciales de la consola).

## 0. Requisitos
- Usuario IAM con permisos sobre S3, RDS, DocumentDB, Redshift Serverless, EC2, Secrets Manager e IAM (roles y perfiles de instancia).
- Terraform ≥ 1.6 en CloudShell:
  ```bash
  mkdir -p ~/bin && curl -sSLo /tmp/tf.zip https://releases.hashicorp.com/terraform/1.9.8/terraform_1.9.8_linux_amd64.zip \
    && unzip -o -q /tmp/tf.zip terraform -d ~/bin && terraform version
  ```
- Este repositorio copiado en CloudShell (`Acciones → Cargar archivo` con un `.tar.gz`, o `git clone`).

## 1. Recursos creados por consola (una sola vez)
1. **S3**: bucket `dw-user3-ecommerce-<fecha>` en us-east-2; bloqueo de acceso público activado, versionado, cifrado SSE-S3, sin Object Lock; política que niega conexiones sin HTTPS (`aws:SecureTransport = false`).
2. **IAM**: rol `dw-user3-redshift-s3-role` (confianza: Redshift) con política insertada de lectura sobre el bucket y `raw/*`.
3. **Redshift Serverless** (configuración personalizada): namespace `dw-user3-namespace` (admin `dwadmin`, contraseña administrada por Secrets Manager, rol anterior **como predeterminado**); workgroup `dw-user3-workgroup` con capacidad base mínima, **sin acceso público**; límite de uso diario de 5 RPU-hora con acción "desactivar".
4. En el Editor de consultas v2 (BD `dev`), ejecutar `redshift/sql/01_esquemas_y_raw.sql` y `02_auditoria.sql`.

## 2. Infraestructura con Terraform
```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars
#  bucket_name, redshift_secret_arn (aws secretsmanager list-secrets),
#  ssh_allowed_cidr = "$(curl -s https://checkip.amazonaws.com)/32"
terraform init
terraform plan -out tfplan      # revisar: solo "to add"
terraform apply tfplan
terraform output
```
Crea: security groups (EC2, fuentes, Redshift), RDS MySQL, DocumentDB, secreto de DocumentDB,
rol y perfil de la EC2 y la EC2 (`dw-user3-pipeline`).

Agregar el SG de Redshift al workgroup (creado por consola):
```bash
aws redshift-serverless update-workgroup --workgroup-name dw-user3-workgroup \
  --security-group-ids <sg-default> $(terraform output -raw redshift_sg_id)
```
Respaldar el estado (lo necesita `destroy`): `aws s3 cp terraform.tfstate s3://<bucket>/terraform/`.

## 3. Preparar la EC2
```bash
aws ec2-instance-connect ssh --instance-id <ec2_instance_id> --connection-type direct --os-user ec2-user
```
En la EC2:
```bash
# código
aws s3 cp s3://<bucket>/deploy/de-dw-aws.tar.gz ~/ && tar xzf ~/de-dw-aws.tar.gz -C ~
# ~/.dw-aws.conf: SOLO endpoints y ARNs (ver .env.aws.example); nunca contraseñas
python3.11 -m venv ~/venv && ~/venv/bin/pip install -r ~/de-dw-aws/scripts/aws/requirements-ec2.txt
cd ~/de-dw-aws && source ~/venv/bin/activate && source scripts/aws/load_env.sh   # lee claves de Secrets Manager

# 3.1 fuentes: esquema y datos (mismos datos que la versión local)
MYSQL_PWD="$MYSQL_PASSWORD" mysql --ssl -h "$MYSQL_HOST" -u "$MYSQL_USER" ecommerce < src/ecommerce_dw/sources/mysql/schema.sql
python -m ecommerce_dw.sources.generate_data
```

## 4. Airflow en la EC2
```bash
sudo mkdir -p /usr/local/lib/docker/cli-plugins
sudo curl -sSfL https://github.com/docker/compose/releases/download/v2.29.7/docker-compose-linux-x86_64 \
  -o /usr/local/lib/docker/cli-plugins/docker-compose && sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
cd ~/de-dw-aws/docker
cp .env.aws.example .env.aws && chmod 600 .env.aws    # completar (claves de Airflow aleatorias, ARNs)
docker compose -f docker-compose.aws.yml --env-file .env.aws up -d --build
```
Interfaz web (opcional): en `terraform.tfvars` poner `airflow_ui_cidr = "<tu IP>/32"`, `terraform apply`
y abrir `http://<IP pública de la EC2>:8080`.

## 5. Ejecutar el pipeline
```bash
AF='docker compose -f docker-compose.aws.yml --env-file .env.aws exec -T airflow-scheduler airflow'
$AF dags unpause dw_aws_pipeline
$AF dags trigger dw_aws_pipeline                                    # siguiente lote libre (0 = inicial)
$AF dags trigger dw_aws_pipeline -c '{"batch_id": 1, "reprocess": true}'   # reprocesar desde S3
```
Nuevos datos en las fuentes: `python -m ecommerce_dw.sources.append_batch --batch-id N`.
Cambio SCD2: `python -m ecommerce_dw.sources.simulate_changes`.
Estado: `select * from audit.pipeline_runs order by started_at;` y `select * from audit.batch_log;`.

## 6. Pausar (sin borrar)
```bash
aws ec2 stop-instances --instance-ids <ec2_instance_id>
aws rds stop-db-instance --db-instance-identifier dw-user3-mysql
aws docdb stop-db-cluster --db-cluster-identifier dw-user3-docdb
```
⚠️ AWS reinicia RDS y DocumentDB detenidos después de 7 días.
Redshift Serverless no se pausa: no cobra cómputo mientras no hay consultas.

## 7. Eliminación de recursos
Todos los recursos están en Terraform (los que creé por consola los importé con `imports.tf`).
```bash
cd infra/terraform
# 1. Vaciar el bucket (está versionado: borrar también versiones y marcas de borrado)
B=dw-user3-ecommerce-20261027
aws s3api delete-objects --bucket $B --delete "$(aws s3api list-object-versions --bucket $B \
  --query '{Objects: [Versions[].{Key:Key,VersionId:VersionId}, DeleteMarkers[].{Key:Key,VersionId:VersionId}][]}' --output json)"
#    (guardar antes una copia local de terraform.tfstate: está dentro del bucket)
# 2. Destruir todo: EC2, RDS, DocumentDB, secretos, SGs, roles, Redshift Serverless y el bucket
terraform destroy
```
El secreto de Redshift lo administra Redshift y se borra con el namespace; el de RDS, con la instancia.
Verificar que no queden recursos: `aws resourcegroupstaggingapi get-resources --tag-filters Key=owner,Values=de-gr01-user-3`.
