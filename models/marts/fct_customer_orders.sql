-- logical ctes
---- staging
with customers as (
    select
        customer_id,
        first_name || ' ' || last_name as full_name,
        last_name as surname,
        first_name as givenname
    from {{ref('stg_jaffle_shop_customers')}}
),

orders as (
    select
        row_number() over(partition by customer_id order by order_date, order_id) as user_order_seq,
        order_id,
        customer_id,
        order_date,
        status as order_status,
        _etl_loaded_at
    from {{ref('stg_jaffle_shop_orders')}}
),

payments as (
    select
        payment_id,
        order_id,
        amount as payment_amount,
        payment_method,
        status as payment_status,
        _batched_at,
        created_at as payment_created_at
    from {{ref('stg_stripe_payments')}}
),
--- marts
customer_order_history as (
    select
        customers.customer_id,
        customers.full_name,
        customers.surname,
        customers.givenname,
        min(order_date) as first_order_date,
        min(case when orders.order_status not in ('returned', 'return_pending') then order_date end) as first_non_returned_order_date,
        max(case when orders.order_status not in ('returned', 'return_pending') then order_date end) as most_recent_non_returned_order_date,
        coalesce(max(user_order_seq),0) as order_count,
        coalesce(count(case when orders.order_status != 'returned' then 1 end),0 ) as non_returned_order_count,
        sum(case when orders.order_status not in ('returned', 'return_pending') then payments.payment_amount else 0 end) as total_lifetime_value,
        sum(case when orders.order_status not in ('returned', 'return_pending') then payments.payment_amount else 0 end)/nullif(count(case when orders.order_status not in ('returned', 'return_pending') then 1 end),0) as avg_non_returned_order_value,
        array_agg(distinct orders.order_id) as order_ids
    from orders
    join customers on orders.customer_id = customers.customer_id
    left outer join payments on orders.order_id = payments.order_id
    where orders.order_status not in ('pending') and payments.payment_status != 'fail'
    group by customers.customer_id, customers.full_name, customers.surname, customers.givenname
),

-- final CTE
final as (
    select
    orders.order_id,
    orders.customer_id,
    customers.surname,
    customers.givenname,
    first_order_date,
    order_count,
    total_lifetime_value,
    payments.payment_amount as order_value_dollars, --need to be fixed as will agg multiple payments
    orders.order_status,
    payments.payment_status
from orders
join customers on orders.customer_id = customers.customer_id
join customer_order_history on orders.customer_id = customer_order_history.customer_id
left outer join payments on orders.order_id = payments.order_id
where payments.payment_status != 'fail'

)

-- final select

select * from final

