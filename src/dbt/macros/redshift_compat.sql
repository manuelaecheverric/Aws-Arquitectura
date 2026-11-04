{# =====================================================================
   Compatibilidad DuckDB -> Amazon Redshift (versión AWS del proyecto).
   Cada macro reemplaza una construcción de DuckDB que Redshift no admite.
   ===================================================================== #}

{# Redshift no admite IS DISTINCT FROM: comparación que trata NULL como valor. #}
{% macro is_distinct(a, b) -%}
(({{ a }}) <> ({{ b }}) or (({{ a }}) is null and ({{ b }}) is not null) or (({{ a }}) is not null and ({{ b }}) is null))
{%- endmacro %}

{# DuckDB: timezone(zona, timestamptz). Redshift: convert_timezone no acepta timestamptz con dos
   argumentos; AT TIME ZONE sobre un timestamptz devuelve la hora local (timestamp sin zona). #}
{% macro to_business_tz(ts) -%}
(({{ ts }}) at time zone '{{ var("business_timezone") }}')
{%- endmacro %}

{# Día de la semana ISO (lunes=1 ... domingo=7). Redshift: dow va de 0 (domingo) a 6. #}
{% macro iso_dow(d) -%}
(case when extract(dow from {{ d }}) = 0 then 7 else extract(dow from {{ d }}) end)
{%- endmacro %}

{# Redshift no convierte texto a boolean con CAST. #}
{% macro text_to_boolean(expr) -%}
(case when lower(trim({{ expr }})) in ('true', 't', '1') then true
      when lower(trim({{ expr }})) in ('false', 'f', '0') then false end)
{%- endmacro %}

{# En AWS "raw" acumula lotes (columna _batch_id). Staging expone una fila por llave de
   negocio: la versión del lote más reciente, equivalente al raw "completo" de la versión local.
   Uso: from {{ latest_version(source('x', 'y'), 'llave') }}  (subconsulta + filtro; Redshift
   rechazó QUALIFY en este contexto). #}
{% macro latest_version(relation, business_key) -%}
(
    select *, row_number() over (
        partition by {{ business_key }}
        order by _batch_id desc, _loaded_at desc
    ) as _version_rank
    from {{ relation }}
) as latest
where latest._version_rank = 1
{%- endmacro %}
