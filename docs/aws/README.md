# Data Warehouse en AWS — documentación

Migré el data warehouse local (repositorio DE-DW) a AWS: usé RDS for MySQL y DocumentDB como
fuentes, S3 como zona de aterrizaje por lotes, Redshift Serverless como almacén, y Airflow y dbt en
una EC2. Consulto los resultados con SQL sobre `marts` (Redshift Query Editor v2). Región us-east-2.

| Documento | Contenido |
|---|---|
| [arquitectura.md](arquitectura.md) | Diagrama, componentes, flujo de un lote, red y accesos |
| [despliegue.md](despliegue.md) | Instrucciones reproducibles de despliegue, ejecución, pausa y eliminación |
| [decisiones.md](decisiones.md) | Decisiones técnicas y diferencias frente a la versión local (incluye MongoDB vs DocumentDB) |
| [calidad_lotes.md](calidad_lotes.md) | Identificación de lotes, marcas de avance, escenarios de calidad y evidencias |
| [costos_seguridad.md](costos_seguridad.md) | Estimación de costos y controles de seguridad |
| [consultas_reporte.md](consultas_reporte.md) | Vista de reporte y consultas de validación del indicador de ventas |

## Mapa del código

| Ruta | Qué contiene |
|---|---|
| `infra/terraform/` | Infraestructura como código (red, RDS, DocumentDB, secretos, EC2, roles) |
| `redshift/sql/` | Esquemas, tablas `raw` y tablas de auditoría |
| `src/ecommerce_dw/aws/` | Extracción a S3, carga con `COPY`, Data API, secretos, tareas del DAG |
| `src/airflow/dags/dw_aws_pipeline.py` | DAG de punta a punta |
| `src/dbt/` | Proyecto dbt (perfil `aws` para Redshift; macros de compatibilidad en `macros/redshift_compat.sql`) |
| `docker/Dockerfile.aws`, `docker/docker-compose.aws.yml` | Airflow en la EC2 |
| `.env.aws.example`, `docker/.env.aws.example`, `infra/terraform/terraform.tfvars.example` | Variables requeridas, sin secretos |
