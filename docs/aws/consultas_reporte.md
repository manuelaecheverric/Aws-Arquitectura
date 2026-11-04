# Consultas de reporte sobre `marts`

> El profesor indicó (3 nov 2026) que el taller **ya no usa Amazon QuickSight**. El punto 3.6 queda
> fuera del alcance; consulto los datos con SQL en **Redshift Query Editor v2**.

## Vista de reporte
Vista `marts.rpt_sales_dashboard` (modelo dbt `rpt_sales_dashboard`): una fila por línea de venta con
fecha, categoría, producto, canal, departamento, ciudad, unidades y ventas. Consume solo `marts`.
La categoría y la ciudad son las de la versión vigente al momento de la venta (SCD2).

## Consultas de ejemplo
```sql
-- Ventas por mes
select anio, mes, sum(ventas) as ventas
from marts.rpt_sales_dashboard
group by anio, mes order by anio, mes;

-- Ventas por categoría
select categoria, sum(ventas) as ventas
from marts.rpt_sales_dashboard
group by categoria order by ventas desc;
```

## Validación del indicador de ventas totales
La vista y el hecho deben dar el mismo total, con los mismos filtros:
```sql
-- Ventas totales sin filtros
select sum(subtotal) as ventas_totales from marts.fct_sales;
select sum(ventas)   as ventas_totales from marts.rpt_sales_dashboard;

-- Ventas totales con filtro, por ejemplo canal = 'web'
select sum(s.subtotal) as ventas_totales
from marts.fct_sales as s
inner join marts.dim_sales_channel as c on c.sales_channel_sk = s.sales_channel_sk
where c.sales_channel = 'web';
select sum(ventas) as ventas_totales from marts.rpt_sales_dashboard where canal = 'web';

-- Fecha de actualización de los datos
select max(started_at) as ultima_ejecucion, max(batch_id) as ultimo_lote
from audit.pipeline_runs where status = 'success';
```
Valor medido tras los lotes 0–2: **7.357.260,42**. Tras el lote 3 (3 nov 2026): **7.420.730,58** en `fct_sales`, con 259 filas en la vista.

## Uso en la demostración
Ejecutar la consulta de ventas totales y la de fecha de actualización **antes y después** de procesar
un lote nuevo: el total cambia solo por las ventas del lote y `ultimo_lote` avanza.
