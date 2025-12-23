with payments as (
     select * 
     from {{ref('stg_stripe_payments')}}
     where status != 'failed'
),
orders as ( select * from {{ref('stg_jaffle_shop_orders')}}),

order_totals as (
    select
        order_id,
        status as payment_status,
        sum(amount) as order_value_dollars
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
)

select * from order_values_joined