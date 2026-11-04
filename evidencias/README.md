# Evidencias de funcionamiento

Organizo las capturas y los logs por punto del taller. Los logs (`.log`) los guardé en S3 durante cada ejecución.

| Requisito de la guía | Evidencia |
|---|---|
| 3.1 Fuentes en RDS for MySQL y DocumentDB | [3.1-fuentes/](3.1-fuentes/): configuración y seguridad de RDS, DocumentDB y EC2; security groups; secretos; rol de la EC2 |
| 3.2 Extracción por lotes a S3 | [3.2-s3/](3.2-s3/): bucket, archivo del lote 0, manifiesto, justificación del formato, log de extracción |
| 3.3 Redshift con `COPY` y rol IAM | [3.3-redshift-dbt/](3.3-redshift-dbt/) 01–11 |
| Resultados de las pruebas de dbt | [3.3-redshift-dbt/](3.3-redshift-dbt/) 12–18 (129/129 pruebas en verde) |
| 3.4 Ejecución exitosa del pipeline en Airflow | [3.4-airflow/](3.4-airflow/) |
| Carga inicial | [3.5-calidad/](3.5-calidad/) 01–04 |
| Registros defectuosos (rechazos con motivo y lote) | [3.5-calidad/](3.5-calidad/) 05 — 32 rechazos en 6 tablas |
| Nuevo lote incremental | [3.5-calidad/](3.5-calidad/) 06–07 — ventas 253 → 256 |
| Repetición de un lote exitoso (idempotencia) | [3.5-calidad/](3.5-calidad/) 06, 08 — mismos conteos y total |
| Cambio en atributo historizado (SCD2) | [3.5-calidad/](3.5-calidad/) 09–11 |
| Fallo controlado y recuperación | [3.5-calidad/](3.5-calidad/) 12–16 |
| Conciliación del indicador de ventas | [3.5-calidad/](3.5-calidad/) 17–18 — 7.420.730,58 tras el lote 3 |
| Costos | [costos/](costos/) |

El detalle de cada escenario está en [docs/aws/calidad_lotes.md](../docs/aws/calidad_lotes.md).
