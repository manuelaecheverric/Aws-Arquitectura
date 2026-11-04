{# Filtro de cargas incrementales para la fila candidata (alias candidate). Carga las llaves nuevas, las filas cuya fecha de negocio cae en la ventana de reproceso (desde incremental_lookback_days antes de la fecha máxima ya cargada) y las filas cuyas llaves de dimensión resueltas cambiaron (tracked_columns), por ejemplo cuando una versión SCD2 nueva reemplaza a la anterior. #}
{% macro incremental_candidate_filter(key_column, business_date_column, tracked_columns=[]) %}
{% if is_incremental() %}
{#- Redshift: no admite subconsultas correlacionadas (EXISTS) en este patrón; se usa LEFT JOIN
    contra la tabla ya cargada y una ventana de reproceso calculada una sola vez. -#}
left join {{ this }} as existing
    on existing.{{ key_column }} = candidate.{{ key_column }}
cross join (
    select dateadd(day, -{{ var('incremental_lookback_days') }}, max(dates.date)) as lookback_start
    from {{ this }} as loaded
    inner join {{ ref('dim_date') }} as dates on dates.date_sk = loaded.date_sk
) as reprocess_window
where existing.{{ key_column }} is null
or candidate.{{ business_date_column }} >= reprocess_window.lookback_start
{% if tracked_columns %}
or (
    {% for column in tracked_columns %}
    {{ is_distinct('existing.' ~ column, 'candidate.' ~ column) }}{{ "" if loop.last else " or" }}
    {% endfor %}
)
{% endif %}
{% endif %}
{% endmacro %}

{# Elimina de la tabla de hechos las filas cuya llave de origen ya no existe en la capa validada. #}
{% macro delete_orphaned_facts(sk_column, source_relation, source_key) %}
delete from {{ this }}
where {{ sk_column }} not in (
    select {{ dbt_utils.generate_surrogate_key(['cast(' ~ source_key ~ ' as varchar)']) }}
    from {{ source_relation }}
)
{% endmacro %}
