# Estimación de costos y controles de seguridad

## Costos (us-east-2, precios de lista bajo demanda; verificar en AWS Pricing Calculator)

| Recurso | Tamaño | Precio de referencia | Encendido 1 h | 24/7 un mes | Detenido un mes |
|---|---|---|---|---|---|
| EC2 | t3.large + 30 GB gp3 + IPv4 pública | 0,0832 USD/h + 0,08 USD/GB-mes + 0,005 USD/h | 0,088 | ≈ 66 | ≈ 2,40 (disco) |
| RDS for MySQL | db.t4g.micro, 20 GB gp3 | 0,016 USD/h + 0,115 USD/GB-mes | 0,016 | ≈ 14 | ≈ 2,30 (almacenamiento) |
| DocumentDB | 1 × db.t3.medium | ≈ 0,078 USD/h + 0,10 USD/GB-mes + 0,20 USD/millón E/S | 0,078 | ≈ 57 | centavos |
| Redshift Serverless | base mínima (4 RPU) | 0,375 USD/RPU-h, por segundo, mínimo 60 s por consulta | solo con consultas | según uso | centavos (almacenamiento) |
| S3 | < 1 MB de datos | 0,023 USD/GB-mes | ≈ 0 | ≈ 0 | ≈ 0 |
| Secrets Manager | 3 secretos | 0,40 USD/secreto-mes | — | 1,20 | 1,20 |
| **Total** | | | **≈ 0,18 USD/h** + Redshift | **≈ 140 USD/mes** | **≈ 6 USD/mes** |

**Consumo real que observé** durante el taller: Redshift Serverless consumió centavos del crédito de
prueba de 300 USD (panel de Redshift); cada sesión de trabajo con fuentes y EC2 encendidas costó
entre 0,05 y 0,20 USD. Por eso mi regla es **detener EC2, RDS y DocumentDB al terminar**
(sección 6 de `despliegue.md`).

Controles de costo que implementé:
- Tamaños mínimos (db.t4g.micro, una sola instancia de DocumentDB, Redshift con capacidad base mínima).
- Límite de uso diario de Redshift Serverless (5 RPU-hora → desactiva el uso).
- Sin NAT Gateway ni balanceadores; sin Multi-AZ; retención de backups de 1 día; sin snapshot final al destruir.
- Etiquetas `owner`, `project`, `managed` en todos los recursos para identificarlos en la cuenta compartida.

## Controles de seguridad

| Control | Implementación |
|---|---|
| Datos privados | Bucket con bloqueo de acceso público y política que niega todo acceso sin HTTPS; RDS, DocumentDB y Redshift sin acceso público |
| Cifrado en reposo | S3 SSE-S3; RDS, DocumentDB, discos EBS y Redshift cifrados |
| Cifrado en tránsito | TLS obligatorio en DocumentDB; `--ssl` en MySQL; `sslmode=require` en dbt; HTTPS hacia S3 y la Data API |
| Sin secretos en el código | Contraseñas en Secrets Manager (RDS y Redshift administran la suya); el repositorio solo tiene ARNs y archivos `*.example`; `.gitignore` excluye `.env*`, `*.tfvars` con datos, `*.tfstate` |
| Mínimo privilegio | Rol de Redshift: solo lectura de `raw/*`. Rol de la EC2: lectura/escritura del bucket del taller, lectura de 3 secretos concretos, Data API. Sin llaves de acceso en la EC2 (perfil de instancia, IMDSv2) |
| Segmentación de red | Security groups por función: las fuentes y Redshift solo aceptan tráfico desde el SG de la EC2 |
| Acceso administrativo | SSH solo desde una IP (/32) con llaves temporales de 60 s (EC2 Instance Connect); interfaz de Airflow cerrada salvo a una IP |
| Trazabilidad | `audit.batch_log`, `audit.pipeline_runs`, manifiestos por lote en S3 y logs de Airflow con `batch_id` y tarea |
| Protección de lotes | Escritura condicional en S3 y versionado del bucket |
| Permisos de la cuenta | Trabajo con los permisos del grupo `de-gr01-students` (sin administrador); documenté los permisos faltantes y las alternativas que usé |
