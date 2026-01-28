{% set identifiers = var('table_identifiers', 'events_* pseudonymous_users_* users_*').split(' ') %}
{% set all_suffixes = [] %}
{% for id in identifiers %}
{% if '_' in id %}
{% set suffix = id.split('_')[-1] %}
{% set all_suffixes = all_suffixes.append(suffix) %}
{% endif %}
{% endfor %}
{% set unique_suffixes = all_suffixes | unique | list %}
{% if unique_suffixes and unique_suffixes[0] != '*' %}
{% set partitions_to_replace = [] %}
{% for suffix in unique_suffixes %}
{% set partitions_to_replace = partitions_to_replace.append("parse_date('%Y%m%d', '" + suffix + "')") %}
{% endfor %}
{% else %}
{% set partitions_to_replace = ['current_date'] %}
{% for i in range(var('static_incremental_days')) %}
    {% set partitions_to_replace = partitions_to_replace.append('date_sub(current_date, interval ' + (i+1)|string + ' day)') %}
{% endfor %}
{% endif %}
{{
    config(
        enabled = true if var('derived_session_properties', false) else false,
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        tags = ["incremental"],
        partition_by={
            "field": "session_partition_date",
            "data_type": "date",
            "granularity": "day",
            "copy_partitions": true

        },
    )
}}


with unnest_event_params as
(
    select 
        session_partition_key
        ,event_date_dt as session_partition_date
        ,event_timestamp
        {% for sp in var('derived_session_properties', []) %}
            {% if sp.user_property %}
                , {{ ga4.unnest_key('user_properties', sp.user_property, sp.value_type) }}
            {% else %}
                , {{ ga4.unnest_key('event_params', sp.event_parameter, sp.value_type) }}
            {% endif %}
        {% endfor %}
    from {{ref('stg_ga4__events')}}
    where event_key is not null
    {% if is_incremental() %}
            and event_date_dt in ({{ partitions_to_replace | join(',') }})
    {% endif %}

)

SELECT DISTINCT
    session_partition_key
    ,session_partition_date
    {% for sp in var('derived_session_properties', []) %}
        , LAST_VALUE({{ sp.user_property | default(sp.event_parameter) }} IGNORE NULLS) OVER (session_window) AS {{ sp.session_property_name }}
    {% endfor %}
FROM unnest_event_params
WINDOW session_window AS (PARTITION BY session_partition_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
