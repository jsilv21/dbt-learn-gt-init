-- This needs to be updated to have the actual drop statement, but for now stopping as I don't want to use it.
-- reference: https://discourse.getdbt.com/t/clean-your-warehouse-of-old-and-deprecated-models/1547/15

{% macro clean_stale_models(database=target.database, schema=target.schema) %}
{% set variable%}
    SELECT table_type,
            table_schema,
            table_name,
            last_altered
    FROM {{ database }}.INFORMATION_SCHEMA.TABLES
    WHERE table_schema = '{{ schema }}'
    ORDER BY last_altered DESC
{%endset%}
{%endmacro%}