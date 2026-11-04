-- Vista de reporte de ventas: una fila por línea de venta con sus atributos de análisis.
-- La categoría y la ciudad son las de la VERSIÓN vigente al momento de la venta (SCD2),
-- porque se unen por la llave subrogada guardada en el hecho.
{{ config(materialized='view') }}

select
    sales.sale_sk,
    dates.date as fecha_venta,
    dates.year as anio,
    dates.month as mes,
    dates.day_of_week as dia_semana,
    dates.is_holiday as es_festivo,
    products.category as categoria,
    products.name as producto,
    channels.sales_channel as canal,
    geography.department as departamento,
    geography.city as ciudad,
    sales.quantity as unidades,
    sales.subtotal as ventas
from {{ ref('fct_sales') }} as sales
inner join {{ ref('dim_date') }} as dates on dates.date_sk = sales.date_sk
inner join {{ ref('dim_product') }} as products on products.product_sk = sales.product_sk
inner join {{ ref('dim_sales_channel') }} as channels on channels.sales_channel_sk = sales.sales_channel_sk
left join {{ ref('dim_geography') }} as geography on geography.geography_sk = sales.geography_sk
