-- imports
with customers as ( select * from {{ref('stg_jaffle_shop_customers')}}),
orders as (select * from {{ref('int_orders')}}),

--
customer_orders as (
    select 
        orders.*,
        customers.full_name,
        customers.last_name,
        customers.first_name,
        -- cust level aggs
        min(order_date) over (partition by orders.customer_id) as customer_first_order_date,
        min(valid_order_date) over (partition by orders.customer_id) as customer_first_non_returned_order_date,
        max(valid_order_date) over (partition by orders.customer_id) as customer_most_recent_non_returned_order_date,
        count(*) over (partition by orders.customer_id) as customer_order_count,
        sum(nvl2(valid_order_date,1,0)) over (partition by orders.customer_id) as customer_non_returned_order_count,
        sum(nvl2(valid_order_date,order_value_dollars,0)) over (partition by orders.customer_id) as customer_total_lifetime_value,
        array_agg(distinct orders.order_id) over (partition by orders.customer_id) as customer_order_ids
    from orders
    inner join customers on orders.customer_id = customers.customer_id
),

customer_average_order_value as (select 
    *,
    customer_total_lifetime_value / NULLIF(customer_non_returned_order_count, 0) as customer_avg_non_returned_order_value --! fix safe div by zero
    from customer_orders
)

select * from customer_average_order_value