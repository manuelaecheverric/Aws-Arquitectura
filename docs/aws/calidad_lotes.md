# Lotes, calidad, incrementalidad y recuperación (3.5)

## Cómo se identifica un lote y cuándo cambia su estado

| Momento | Dónde queda | Estado / marca |
|---|---|---|
| Se decide el lote | Parámetro `batch_id` del DAG (−1 = `max(batch_id)+1` de `audit.batch_log`) | — |
| Inicio de la ejecución | `audit.pipeline_runs` | `running` |
| Cada archivo escrito en S3 | `raw/<fuente>/<entidad>/batch_id=N/` | — |
| Todos los archivos escritos | `control/batches/batch_id=N/manifest.json` (se escribe **al final**) | lote **completo** en S3 |
| Registro de la extracción | `audit.batch_log` (una fila por entidad) | `extracted` + `record_count` |
| `COPY` de una entidad confirmado | Misma transacción: `"raw"`, `audit.batch_log`, `audit.extraction_watermark` | `loaded` + `loaded_count`; la **marca de avance** pasa al último id extraído |
| Fallo en el `COPY` | Transacción revertida | `failed` + `error_message`; la marca **no** avanza |
| Fin del DAG | `audit.pipeline_runs` (tarea `register_result`, se ejecuta siempre) | `success` o `failed` + `failed_task` |

La marca de avance solo se mueve cuando el lote **quedó cargado en Redshift**. Si algo falla
antes, la siguiente extracción incremental vuelve a traer esos registros: no se pierde nada.

## Por qué no hay duplicados
- **S3**: un lote completo no se puede sobrescribir (escritura condicional); un lote incompleto se reemplaza y el versionado guarda lo anterior.
- **raw**: cada `COPY` borra antes las filas de su `_batch_id` dentro de la misma transacción.
- **staging**: una fila por llave de negocio (la del lote más reciente).
- **hechos**: incrementales con `merge` por llave subrogada (`sale_sk`, `review_sk`, `navigation_event_sk`).
- **SCD2**: los snapshots solo crean versión cuando `updated_at` avanza.

## Evidencias de los escenarios

| Escenario | Cómo lo probé | Resultado observado | Evidencia (`s3://…/evidencias/`) |
|---|---|---|---|
| Carga inicial | Lote 0 (extracción + `COPY` + `dbt build`) | 253 ventas, 150 reseñas, 500 eventos en `marts`; 129/129 pruebas | `lote0_*.log`, `dbt_build_E.log` |
| Registros defectuosos | 34 filas defectuosas inyectadas por el generador | Rechazos en `quarantine.*` con `rejection_reason` y `_batch_id`. **32 rechazos en las 6 tablas** (16 reglas × 2), verificado el 3 nov 2026. De las 34 filas inyectadas, 2 son cabeceras de pedido válidas (sirven para aislar `subtotal_mismatch`), así que 32 es el total esperado. | `dbt_verificacion.log`, captura `evidencias/consulta 2.png` |
| Repetición de un lote exitoso | `COPY` del lote 0 dos veces; DAG con `batch_id=1, reprocess=true` | Mismos conteos en `raw` y `marts`; total de ventas igual | `lote0_load.log`, `dag_reproceso.log` |
| Nuevo lote incremental | `append_batch --batch-id 1` + DAG (lote 1) | Solo 3 pedidos, 3 líneas, 3 reseñas, 3 eventos nuevos; ventas 253 → 256 | `dag_lote1.log` |
| Cambio en atributo historizado | `simulate_changes` (cliente 1 y producto 1) + DAG (lote 2) | Cliente 1: versión Bogotá cerrada el 2025-07-01, Medellín vigente; producto 1: Electrónica → Hogar | `scd2_cambio.log`, `scd2_resultado.log` |
| Fallo controlado y reejecución | `append_batch --batch-id 3`; DAG `{"batch_id": 3, "fail_task": "load_to_redshift"}` y luego `{"batch_id": 3, "reprocess": true}` | 1ª ejecución: extracción OK (`extracted`), carga falla tras 2 reintentos, `pipeline_runs` = **failed** con `failed_task = load_to_redshift`; la marca de avance se queda en el lote 2 y los hechos no cambian (256). Reejecución desde S3 (sin consultar fuentes): **success**, marca en lote 3, hechos +3 (ventas 259, reseñas 156, eventos 506) | `fallo_1_estado.log`, `fallo_2_final.log`, `fallo_3_recuperacion.log` |

## Pruebas de calidad (dbt)
- **Unicidad y no nulos**: llaves de staging, llaves subrogadas de dimensiones y hechos.
- **Campos obligatorios**: `not_null` en llaves foráneas de los hechos.
- **Relaciones**: hechos → `dim_customer`, `dim_product`, `dim_date`, `dim_sales_channel`, `dim_geography`, `dim_event_type`.
- **Reglas de negocio**: `accepted_values` de `rejection_reason`, canales y tipos de evento; fecha de negocio en hora de Bogotá; fechas de venta iguales a las de raw.
- **Conciliación**: `assert_sales_totals_match_source_by_business_date` compara por día el subtotal de `fct_sales`
  con la fuente validada. Verifiqué el total global: origen válido = modelo = **7.293.790,26** (lote 0) y
  **7.357.260,42** tras el lote 1. Tras el lote 3: **7.420.730,58** (259 ventas; misma cifra y conteo en `marts.rpt_sales_dashboard`), captura `evidencias/consulta 1.1.png` y `1.2.png`.

### Conteo completo de la cuarentena
Resultado que obtuve (3 nov 2026): 16 combinaciones entidad/motivo con 2 filas cada una = **32 rechazos**.
customers (invalid_email, missing_name) y products (missing_category) aparecen con `_batch_id` = 3 y
no 0: clientes y productos se extraen **completos en cada lote**, y staging toma la última versión de
cada llave, así que el rechazo queda asociado al último lote donde llegó el registro. Los hechos
(pedidos, líneas, reseñas, eventos) se extraen por incremento y conservan el lote 0, donde se originaron.
La regla `negative_price` existe en dbt, pero el generador no inyecta productos con precio negativo.

```sql
select 'customers' entidad, rejection_reason, _batch_id, count(*) from quarantine.quarantine_mysql__customers group by 1,2,3
union all select 'products', rejection_reason, _batch_id, count(*) from quarantine.quarantine_mysql__products group by 1,2,3
union all select 'orders', rejection_reason, _batch_id, count(*) from quarantine.quarantine_mysql__orders group by 1,2,3
union all select 'order_items', rejection_reason, _batch_id, count(*) from quarantine.quarantine_mysql__order_items group by 1,2,3
union all select 'reviews', rejection_reason, _batch_id, count(*) from quarantine.quarantine_mongodb__reviews group by 1,2,3
union all select 'navigation_events', rejection_reason, _batch_id, count(*) from quarantine.quarantine_mongodb__navigation_events group by 1,2,3
order by 1, 2;
```
