-- =====================================================================
-- 01 · Esquemas y tablas RAW en Amazon Redshift Serverless
-- Base de datos: dev   ·   Workgroup: dw-user3-workgroup
--
-- Diferencias frente a la versión local (DuckDB):
--  * RAW es palabra reservada en Redshift -> el esquema se escribe "raw".
--  * VARCHAR sin longitud en Redshift = 256 bytes -> se fijan longitudes.
--    Las longitudes son en BYTES (una tilde ocupa 2 bytes en UTF-8).
--  * document_json se guarda como VARCHAR(65535) y se convierte a SUPER
--    con JSON_PARSE en staging (dbt).
--  * Raw ya NO se reemplaza completo: acumula lotes. Cada fila lleva
--    _batch_id; recargar un lote borra primero ese lote (idempotencia).
--  * _batch_id, _extracted_at y _source vienen dentro del CSV (los escribe
--    el script de extracción); _loaded_at lo pone Redshift al hacer COPY.
-- =====================================================================

create schema if not exists "raw";
create schema if not exists intermediate;
create schema if not exists quarantine;
create schema if not exists marts;
create schema if not exists audit;

-- ---------------------------------------------------------------- MySQL
create table if not exists "raw".mysql_customers (
    customer_id        bigint,
    name               varchar(256),
    email              varchar(256),
    phone              varchar(64),
    city               varchar(128),
    department         varchar(128),
    registration_date  date,
    updated_at         timestamp,
    _batch_id          integer       not null,
    _extracted_at      timestamptz   not null,
    _source            varchar(64)   not null,
    _loaded_at         timestamp     default getdate()
);

create table if not exists "raw".mysql_products (
    product_id         bigint,
    name               varchar(256),
    category           varchar(128),
    price              decimal(12,2),
    description        varchar(4096),
    is_active          boolean,
    updated_at         timestamp,
    _batch_id          integer       not null,
    _extracted_at      timestamptz   not null,
    _source            varchar(64)   not null,
    _loaded_at         timestamp     default getdate()
);

create table if not exists "raw".mysql_orders (
    order_id           bigint,
    customer_id        bigint,
    order_date         timestamp,
    status             varchar(64),
    sales_channel      varchar(64),
    payment_method     varchar(64),
    total              decimal(14,2),
    _batch_id          integer       not null,
    _extracted_at      timestamptz   not null,
    _source            varchar(64)   not null,
    _loaded_at         timestamp     default getdate()
);

create table if not exists "raw".mysql_order_items (
    order_item_id      bigint,
    order_id           bigint,
    product_id         bigint,
    quantity           integer,
    unit_price         decimal(12,2),
    subtotal           decimal(14,2),
    _batch_id          integer       not null,
    _extracted_at      timestamptz   not null,
    _source            varchar(64)   not null,
    _loaded_at         timestamp     default getdate()
);

-- -------------------------------------------------------------- MongoDB
create table if not exists "raw".mongo_reviews (
    review_id          bigint,
    customer_id        bigint,
    product_id         bigint,
    rating             integer,
    reviewed_at        timestamptz,
    document_json      varchar(65535),
    _batch_id          integer       not null,
    _extracted_at      timestamptz   not null,
    _source            varchar(64)   not null,
    _loaded_at         timestamp     default getdate()
);

create table if not exists "raw".mongo_navigation_events (
    event_id           bigint,
    event_type         varchar(64),
    occurred_at        timestamptz,
    customer_id        bigint,
    product_id         bigint,
    sales_channel      varchar(64),
    document_json      varchar(65535),
    _batch_id          integer       not null,
    _extracted_at      timestamptz   not null,
    _source            varchar(64)   not null,
    _loaded_at         timestamp     default getdate()
);
