-- Purpose - testing environment specific macro, plus merging to run a prod job
{{
  config(
    materialized='table'
  )
}}

SELECT
    '{{ target.name }}' AS environment_name,
    '{{ target.schema }}' AS schema_name,
    -- Boolean flag using Jinja logic
    {% if target.name == 'prod' %}
        TRUE
    {% else %}
        FALSE
    {% endif %} AS is_production_run,
    -- String label using Jinja logic
    '{{ "PROD_DATA" if target.name == "prod" else "TEST_DATA" }}' AS data_label,
    CURRENT_TIMESTAMP AS run_at