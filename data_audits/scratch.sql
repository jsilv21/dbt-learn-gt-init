SELECT max(order_date) from analytics.dbt_jsilv.fct_orders

SELECT * FROM {{ref('fct_orders')}}