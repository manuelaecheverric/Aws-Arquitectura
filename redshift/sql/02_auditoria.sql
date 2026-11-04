-- =====================================================================
-- 02 · Tablas de control (esquema audit)
-- Responden: ¿qué lotes existen?, ¿cuántos registros trajo cada entidad?,
-- ¿en qué estado quedó cada ejecución y qué tarea falló?,
-- ¿desde dónde debe seguir la próxima extracción incremental?
-- Nota: Redshift acepta PRIMARY KEY pero NO la hace cumplir; es informativa.
-- La unicidad la garantiza el pipeline (delete + insert en una transacción).
-- =====================================================================

-- Un registro por lote y entidad: qué se extrajo y si ya se cargó.
create table if not exists audit.batch_log (
    batch_id        integer        not null,
    source          varchar(32)    not null,   -- mysql | mongodb
    entity          varchar(64)    not null,   -- customers, orders, reviews...
    s3_key          varchar(1024)  not null,   -- raw/mysql/orders/batch_id=3/orders.csv.gz
    record_count    integer        not null,   -- filas escritas en el archivo
    loaded_count    integer,                   -- filas que quedaron en raw tras el COPY
    extracted_at    timestamptz    not null,
    loaded_at       timestamptz,
    status          varchar(16)    not null,   -- extracted | loaded | failed
    dag_run_id      varchar(256),
    error_message   varchar(4096),
    primary key (batch_id, source, entity)
);

-- Un registro por ejecución del DAG: resultado global y tarea que falló.
create table if not exists audit.pipeline_runs (
    dag_run_id      varchar(256)   not null,
    batch_id        integer        not null,
    started_at      timestamptz    not null,
    finished_at     timestamptz,
    status          varchar(16)    not null,   -- running | success | failed
    failed_task     varchar(256),
    message         varchar(4096),
    primary key (dag_run_id)
);

-- Marca de avance de la extracción incremental por entidad.
-- Solo se actualiza cuando el lote quedó cargado en Redshift (status=loaded),
-- así un fallo no "salta" registros en la siguiente ejecución.
create table if not exists audit.extraction_watermark (
    source          varchar(32)    not null,
    entity          varchar(64)    not null,
    watermark_column varchar(64)   not null,   -- updated_at, order_date, _id...
    last_value      varchar(64),               -- último valor extraído con éxito
    last_batch_id   integer,
    updated_at      timestamp      default getdate(),
    primary key (source, entity)
);
