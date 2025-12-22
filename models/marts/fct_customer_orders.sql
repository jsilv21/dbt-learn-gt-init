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
        valid_order_date,
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
    where payment_status != 'fail'
),
----
order_totals as (
    select
        order_id,
        payment_status,
        sum(payment_amount) as order_value_dollars
    from payments
    group by 1,2
),

order_values_joined as(
    select
        orders.*,
        order_totals.payment_status,
        order_totals.order_value_dollars
    from orders
    left join order_totals on orders.order_id = order_totals.order_id
),


--- marts
customer_order_history as (
    select
        customers.customer_id,
        customers.full_name,
        customers.surname,
        customers.givenname,
        min(order_date) as first_order_date,
        min(valid_order_date) as first_non_returned_order_date,
        max(valid_order_date) as most_recent_non_returned_order_date,
        coalesce(max(user_order_seq),0) as order_count,
        coalesce(count(case when orders.valid_order_date is not null then 1 end),0 ) as non_returned_order_count,
        sum(case when orders.valid_order_date is not null then payments.payment_amount else 0 end) as total_lifetime_value,
        sum(case when orders.valid_order_date is not null then payments.payment_amount else 0 end)/nullif(count(case when orders.valid_order_date is not null then 1 end),0) as avg_non_returned_order_value,
        array_agg(distinct orders.order_id) as order_ids
    from orders
    join customers on orders.customer_id = customers.customer_id
    left outer join payments on orders.order_id = payments.order_id
    --where orders.order_status not in ('pending') (REMOVED AS NO DATA IN PENDING STATUS - BUT BE CAREFUL)
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

)

-- final select

select * from final

