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
{% for i in range(env_var('GA4_INCREMENTAL_DAYS')|int if env_var('GA4_INCREMENTAL_DAYS', false) else var('static_incremental_days')) %}
    {% set partitions_to_replace = partitions_to_replace.append('date_sub(current_date, interval ' + (i+1)|string + ' day)') %}
{% endfor %}
{% endif %}
{{
    config(
        materialized = 'incremental',
        unique_key='occurrence_date',
        incremental_strategy='insert_overwrite',
        enabled=false,
        partition_by={
            "field": "occurrence_date",
            "data_type": "date",
        },
        partitions = partitions_to_replace,
    )
}}

with source as (
    select
        pseudo_user_id
        , stream_id 
        {{ ga4.base_select_usr_source() }}
    from {{ source('ga4', 'pseudonymous_users') }}
    {% if unique_suffixes and unique_suffixes[0] != '*' %}
    where _table_suffix in ('{{ unique_suffixes | join("','") }}')
    {% else %}
    {% if is_incremental() %}
        where parse_date('%Y%m%d', left(_table_suffix, 8)) in ({{ partitions_to_replace | join(',') }})
    {% endif %}
    {% endif %}
)

select * from source
