# Decisiones técnicas y diferencias frente a la versión local

## Fuentes (3.1)

| Tema | Local | AWS | Motivo |
|---|---|---|---|
| MySQL | Contenedor `mysql:8.4` | RDS for MySQL 8.0, db.t4g.micro | Servicio administrado más pequeño compatible con el esquema |
| MongoDB | Contenedor `mongo:8.0` | DocumentDB 5.0, una instancia db.t3.medium | Instancia mínima de DocumentDB basado en instancias |
| Datos | `generate_data.py` | El mismo script, ejecutado en la EC2 | Conservo los mismos datos sintéticos (seed 42) y defectos |
| Esquema MySQL | `docker-entrypoint-initdb.d` | `mysql < schema.sql` desde la EC2 | RDS no ejecuta scripts de inicio |

### Ajustes por diferencias entre MongoDB y DocumentDB
- **TLS obligatorio**: la URI incluye `tls=true&tlsCAFile=global-bundle.pem` (CA de AWS descargada en la EC2).
- **`retryWrites=false`**: DocumentDB no admite escrituras reintentables.
- **`replicaSet=rs0&readPreference=secondaryPreferred`**: conexión recomendada para el clúster.
- **Autenticación**: usuario maestro del clúster (no hay `authSource=admin` con usuario root como en local).
- **Validación de esquema**: el validador `$jsonSchema` y la inserción con `bypass_document_validation`
  del generador funcionaron sin cambios en DocumentDB 5.0.
- **Compatibilidad parcial**: pymongo advierte que DocumentDB no es MongoDB; las operaciones usadas
  (`find`, `sort`, `insert_many`, `replace_one`, `collMod`) son compatibles.
- **Sin acceso público**: DocumentDB solo es alcanzable dentro de la VPC (desde la EC2).

## Extracción y S3 (3.2)

| Decisión | Justificación |
|---|---|
| **CSV comprimido con gzip** | Mínimo cambio frente a los CSV locales; `COPY ... FORMAT AS CSV GZIP` es nativo; revisable a mano. Parquet compensa con volúmenes grandes. |
| Ruta `raw/<fuente>/<entidad>/batch_id=<id>/` | Identifica fuente, entidad y lote en la ruta (requisito). |
| Columnas `_batch_id`, `_extracted_at`, `_source` dentro del CSV | Trazabilidad fila a fila hasta el lote; permite registrar rechazos con su lote. |
| Manifiesto `control/batches/batch_id=N/manifest.json` escrito **al final** | Marca el lote como completo; guarda conteos y marcas de avance. Un lote sin manifiesto es un intento fallido. |
| Escritura condicional `If-None-Match: *` | S3 rechaza sobrescribir un archivo de un lote completo: nunca se pisa en silencio. |
| Bucket versionado | Si un lote incompleto se reemplaza, la versión anterior se conserva. |
| Clientes y productos: extracción **completa** en cada lote | Son catálogos pequeños y `simulate_changes.py` usa `updated_at` retroactivo; un filtro "mayor que la última fecha" perdería cambios. El snapshot de dbt detecta qué cambió. |
| Pedidos, líneas, reseñas, eventos: **incremental por id** | Ids crecientes (`append_batch.py` usa ids ≥ 1.000.000). Limitación: no detecta borrados ni ediciones de filas ya extraídas. |

## Redshift (3.3)

| Diferencia | Ajuste |
|---|---|
| DuckDB → Redshift Serverless | Paga por segundo de consulta (mínimo 60 s); capacidad base mínima; límite diario de 5 RPU-hora. Un clúster aprovisionado cobra cada hora encendido. |
| `raw` es palabra reservada | Creo y referencio el esquema como `"raw"` (dbt pone comillas automáticamente). |
| Base de datos | `dev` (nombre fijo del asistente de Redshift Serverless). |
| Raw | Local: se reemplazaba completo. AWS: **acumula lotes**; la carga borra y vuelve a copiar solo su lote, en una transacción. |
| Staging | Toma la **última versión por llave** entre lotes (`row_number()` por `_batch_id desc`); equivale al raw completo local. |
| `VARCHAR` | Sin longitud = 256 bytes; fijé longitudes (en bytes; una tilde ocupa 2). |
| `document_json` | `VARCHAR(65535)`; se lee con `json_extract_path_text`. |
| `SELECT * EXCLUDE` | No existe → `dbt_utils.star(..., except=[...])`. |
| `IS DISTINCT FROM` | No existe → macro `is_distinct` que trata `NULL` como valor. |
| `timezone('America/Bogota', ts)` | → `ts AT TIME ZONE 'America/Bogota'`. |
| `extract(isodow ...)` | → calculado desde `dow` (domingo = 0). |
| CAST texto → boolean | No admitido → `case when ... in ('true','t','1')`. |
| `fecha - 7` | → `dateadd(day, -7, fecha)`. |
| `QUALIFY` en staging | Falló en ese contexto → subconsulta con `row_number()`. |
| Subconsulta correlacionada (`EXISTS`) en el filtro incremental | No soportada → `LEFT JOIN` contra la tabla ya cargada. |
| `incremental_strategy='delete+insert'` | dbt 1.9 genera `DELETE ... AS alias`, inválido en Redshift → `merge`. |
| Autorización de `COPY` | Rol IAM `dw-user3-redshift-s3-role` (solo lectura de `raw/*`) asociado como predeterminado. |
| Rechazos | Tablas `quarantine.*` con `rejection_reason` y `_batch_id` (lote de origen). |

Macros nuevas: `src/dbt/macros/redshift_compat.sql`. El perfil `aws` está en `src/dbt/profiles.yml`
(se activa con `DBT_TARGET=aws`); el perfil `local` (DuckDB) se conserva.

## Orquestación (3.4)

| Local | AWS |
|---|---|
| 3 DAGs (`extract_to_raw`, `transform_warehouse`, `ecommerce_pipeline`) | 1 DAG `dw_aws_pipeline` con 12 tareas |
| Bloqueo de archivo para el único escritor de DuckDB | `max_active_runs=1`: un lote a la vez |
| Credenciales en `.env` | Solo ARNs; cada tarea lee las claves de Secrets Manager con el rol de la EC2 |
| Sin registro de ejecuciones en el almacén | `audit.pipeline_runs` y `audit.batch_log` |

Parámetros del DAG: `batch_id` (-1 = siguiente libre), `mode` (initial/incremental),
`reprocess` (true = recarga desde S3 sin consultar las fuentes). Reintentos: 2, con espera
exponencial (30 s a 5 min). Cada línea de log incluye `[batch_id=N][task=...]`.

## Infraestructura como código

- **Terraform** (`infra/terraform/`): red y security groups, RDS, DocumentDB, secretos, rol y perfil de la EC2, EC2, SG de Redshift.
- Creé por **consola** (para aprender a usarla) el bucket S3, el rol de Redshift y Redshift Serverless; después los
  **importé a Terraform** (`imports.tf` + `console_resources.tf`, plan sin cambios), así `terraform destroy` también los elimina.
- No pude crear un **EC2 Instance Connect Endpoint** (requiere `iam:CreateServiceLinkedRole`
  para ese servicio, no permitido en la cuenta del curso). Usé EC2 Instance Connect con SSH
  restringido a la IP de administración.
- La EC2 ignora cambios de `user_data` y de IP pública en Terraform para no reemplazarse al detenerla.
