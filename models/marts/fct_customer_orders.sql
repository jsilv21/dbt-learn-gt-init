with customer_orders as (
    select * from {{ref('int_customers')}}
),
--- marts

-- final CTE
final as (
    select
    order_id,
    customer_id,
    last_name as surname,
    first_name as given_name,
    customer_first_order_date as first_order_date,
    customer_order_count as order_count,
    customer_total_lifetime_value as total_lifetime_value,
    order_value_dollars, --need to be fixed as will agg multiple payments
    status as order_status,
    payment_status
from customer_orders
)

-- final select

select * from final

