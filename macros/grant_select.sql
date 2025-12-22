{%- macro grant_select(schema=target.schema, role=target.role, database=target.database) -%}
    {%set sql %}
    grant usage on schema {{ schema }} to role {{ role }};
    grant select on all tables in schema {{ schema }} to role {{ role }};
    grant select on all views in schema {{ schema }} to role {{ role }};
    {%endset%}

	{{ log("Granting select on schema: " ~ schema ~ ", role: " ~ role, info=True) }}
    {% do run_query(sql) %}
    {{log ("Select granted on schema: " ~ schema ~ ", role: " ~ role, info=True)}}
{%- endmacro -%}