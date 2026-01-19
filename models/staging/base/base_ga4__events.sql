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
    pre_hook="{{ ga4.combine_property_data() }}" if var('combined_dataset', false) else "",
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
        "field": "event_date_dt",
        "data_type": "date"
    },
    partitions = partitions_to_replace,
            cluster_by=['event_name']

  )
}}

with source as (
    select
        {{ ga4.base_select_source() }}
    from {{ source('ga4', 'events') }}
    {% if unique_suffixes and unique_suffixes[0] != '*' %}
    where replace(_table_suffix, 'intraday_', '') in ('{{ unique_suffixes | join("','") }}')
    {% else %}
    {% if is_incremental() %}
        where parse_date('%Y%m%d', left(replace(_table_suffix, 'intraday_', ''), 8)) in ({{ partitions_to_replace | join(',') }})
    {% endif %}
    {% endif %}
),
renamed as (
    select
        {{ ga4.base_select_renamed() }}
    from source
)

select * from renamed
qualify row_number() over(partition by event_date_dt, stream_id, user_pseudo_id, session_id, event_name, event_timestamp, farm_fingerprint(to_json_string(ARRAY(SELECT AS STRUCT key, value FROM UNNEST(event_params) AS params ORDER BY key)))) = 1
