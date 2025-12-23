select * from (





  

  

  

  

    

    /*  Create a query combining results from all columns so that the user, or the 
    test suite, can examine all at once.
    */
    
    

    /*  Create a CTE that wraps all the unioned subqueries that are created
        in this for loop
    */
      with main as ( 

    

    /*  There will be one audit_query subquery for each column
    */
    ( with a_query as (
          
    select
      *,
      order_id as dbt_audit_helper_pk
    from ANALYTICS.DBT_JSILV.CUSTOMER_ORDERS_LEGACY
  
),

b_query as (
    
    select
      *,
      order_id as dbt_audit_helper_pk
    from ANALYTICS.DBT_JSILV.fct_customer_orders
  
)
    select
        coalesce(a_query.dbt_audit_helper_pk, b_query.dbt_audit_helper_pk) as primary_key,

        
            'ORDER_ID' as column_name,
        

        coalesce(
            a_query.ORDER_ID = b_query.ORDER_ID and 
                a_query.dbt_audit_helper_pk is not null and b_query.dbt_audit_helper_pk is not null,
            (a_query.ORDER_ID is null and b_query.ORDER_ID is null),
            false
        ) as perfect_match,
        a_query.ORDER_ID is null and a_query.dbt_audit_helper_pk is not null as null_in_a,
        b_query.ORDER_ID is null and b_query.dbt_audit_helper_pk is not null as null_in_b,
        a_query.dbt_audit_helper_pk is null as missing_from_a,
        b_query.dbt_audit_helper_pk is null as missing_from_b,
        coalesce(
            a_query.dbt_audit_helper_pk is not null and b_query.dbt_audit_helper_pk is not null and 
            -- ensure that neither value is missing before considering it a conflict
            (
                a_query.ORDER_ID != b_query.ORDER_ID or -- two not-null values that do not match
                (a_query.ORDER_ID is not null and b_query.ORDER_ID is null) or -- null in b and not null in a
                (a_query.ORDER_ID is null and b_query.ORDER_ID is not null) -- null in a and not null in b
            ), 
            false
        ) as conflicting_values
        -- considered a conflict if the values do not match AND at least one of the values is not null.

    from a_query

    full outer join b_query on (a_query.dbt_audit_helper_pk = b_query.dbt_audit_helper_pk)



 )

    

      union all

    

  

    

    /*  Create a query combining results from all columns so that the user, or the 
    test suite, can examine all at once.
    */
    
    

    /*  There will be one audit_query subquery for each column
    */
    ( with a_query as (
          
    select
      *,
      order_id as dbt_audit_helper_pk
    from ANALYTICS.DBT_JSILV.CUSTOMER_ORDERS_LEGACY
  
),

b_query as (
    
    select
      *,
      order_id as dbt_audit_helper_pk
    from ANALYTICS.DBT_JSILV.fct_customer_orders
  
)
    select
        coalesce(a_query.dbt_audit_helper_pk, b_query.dbt_audit_helper_pk) as primary_key,

        
            'CUSTOMER_ID' as column_name,
        

        coalesce(
            a_query.CUSTOMER_ID = b_query.CUSTOMER_ID and 
                a_query.dbt_audit_helper_pk is not null and b_query.dbt_audit_helper_pk is not null,
            (a_query.CUSTOMER_ID is null and b_query.CUSTOMER_ID is null),
            false
        ) as perfect_match,
        a_query.CUSTOMER_ID is null and a_query.dbt_audit_helper_pk is not null as null_in_a,
        b_query.CUSTOMER_ID is null and b_query.dbt_audit_helper_pk is not null as null_in_b,
        a_query.dbt_audit_helper_pk is null as missing_from_a,
        b_query.dbt_audit_helper_pk is null as missing_from_b,
        coalesce(
            a_query.dbt_audit_helper_pk is not null and b_query.dbt_audit_helper_pk is not null and 
            -- ensure that neither value is missing before considering it a conflict
            (
                a_query.CUSTOMER_ID != b_query.CUSTOMER_ID or -- two not-null values that do not match
                (a_query.CUSTOMER_ID is not null and b_query.CUSTOMER_ID is null) or -- null in b and not null in a
                (a_query.CUSTOMER_ID is null and b_query.CUSTOMER_ID is not null) -- null in a and not null in b
            ), 
            false
        ) as conflicting_values
        -- considered a conflict if the values do not match AND at least one of the values is not null.

    from a_query

    full outer join b_query on (a_query.dbt_audit_helper_pk = b_query.dbt_audit_helper_pk)



 )

    

      union all

    

  

    

    /*  Create a query combining results from all columns so that the user, or the 
    test suite, can examine all at once.
    */
    
    

    /*  There will be one audit_query subquery for each column
    */
    ( with a_query as (
          
    select
      *,
      order_id as dbt_audit_helper_pk
    from ANALYTICS.DBT_JSILV.CUSTOMER_ORDERS_LEGACY
  
),

b_query as (
    
    select
      *,
      order_id as dbt_audit_helper_pk
    from ANALYTICS.DBT_JSILV.fct_customer_orders
  
)
    select
        coalesce(a_query.dbt_audit_helper_pk, b_query.dbt_audit_helper_pk) as primary_key,

        
            'SURNAME' as column_name,
        

        coalesce(
            a_query.SURNAME = b_query.SURNAME and 
                a_query.dbt_audit_helper_pk is not null and b_query.dbt_audit_helper_pk is not null,
            (a_query.SURNAME is null and b_query.SURNAME is null),
            false
        ) as perfect_match,
        a_query.SURNAME is null and a_query.dbt_audit_helper_pk is not null as null_in_a,
        b_query.SURNAME is null and b_query.dbt_audit_helper_pk is not null as null_in_b,
        a_query.dbt_audit_helper_pk is null as missing_from_a,
        b_query.dbt_audit_helper_pk is null as missing_from_b,
        coalesce(
            a_query.dbt_audit_helper_pk is not null and b_query.dbt_audit_helper_pk is not null and 
            -- ensure that neither value is missing before considering it a conflict
            (
                a_query.SURNAME != b_query.SURNAME or -- two not-null values that do not match
                (a_query.SURNAME is not null and b_query.SURNAME is null) or -- null in b and not null in a
                (a_query.SURNAME is null and b_query.SURNAME is not null) -- null in a and not null in b
            ), 
            false
        ) as conflicting_values
        -- considered a conflict if the values do not match AND at least one of the values is not null.

    from a_query

    full outer join b_query on (a_query.dbt_audit_helper_pk = b_query.dbt_audit_helper_pk)



 )

    

      union all

    

  

    

    /*  Create a query combining results from all columns so that the user, or the 
    test suite, can examine all at once.
    */
    
    

    /*  There will be one audit_query subquery for each column
    */
    ( with a_query as (
          
    select
      *,
      order_id as dbt_audit_helper_pk
    from ANALYTICS.DBT_JSILV.CUSTOMER_ORDERS_LEGACY
  
),

b_query as (
    
    select
      *,
      order_id as dbt_audit_helper_pk
    from ANALYTICS.DBT_JSILV.fct_customer_orders
  
)
    select
        coalesce(a_query.dbt_audit_helper_pk, b_query.dbt_audit_helper_pk) as primary_key,

        
            'GIVEN_NAME' as column_name,
        

        coalesce(
            a_query.GIVEN_NAME = b_query.GIVEN_NAME and 
                a_query.dbt_audit_helper_pk is not null and b_query.dbt_audit_helper_pk is not null,
            (a_query.GIVEN_NAME is null and b_query.GIVEN_NAME is null),
            false
        ) as perfect_match,
        a_query.GIVEN_NAME is null and a_query.dbt_audit_helper_pk is not null as null_in_a,
        b_query.GIVEN_NAME is null and b_query.dbt_audit_helper_pk is not null as null_in_b,
        a_query.dbt_audit_helper_pk is null as missing_from_a,
        b_query.dbt_audit_helper_pk is null as missing_from_b,
        coalesce(
            a_query.dbt_audit_helper_pk is not null and b_query.dbt_audit_helper_pk is not null and 
            -- ensure that neither value is missing before considering it a conflict
            (
                a_query.GIVEN_NAME != b_query.GIVEN_NAME or -- two not-null values that do not match
                (a_query.GIVEN_NAME is not null and b_query.GIVEN_NAME is null) or -- null in b and not null in a
                (a_query.GIVEN_NAME is null and b_query.GIVEN_NAME is not null) -- null in a and not null in b
            ), 
            false
        ) as conflicting_values
        -- considered a conflict if the values do not match AND at least one of the values is not null.

    from a_query

    full outer join b_query on (a_query.dbt_audit_helper_pk = b_query.dbt_audit_helper_pk)



 )

    

      union all

    

  

    

    /*  Create a query combining results from all columns so that the user, or the 
    test suite, can examine all at once.
    */
    
    

    /*  There will be one audit_query subquery for each column
    */
    ( with a_query as (
          
    select
      *,
      order_id as dbt_audit_helper_pk
    from ANALYTICS.DBT_JSILV.CUSTOMER_ORDERS_LEGACY
  
),

b_query as (
    
    select
      *,
      order_id as dbt_audit_helper_pk
    from ANALYTICS.DBT_JSILV.fct_customer_orders
  
)
    select
        coalesce(a_query.dbt_audit_helper_pk, b_query.dbt_audit_helper_pk) as primary_key,

        
            'FIRST_ORDER_DATE' as column_name,
        

        coalesce(
            a_query.FIRST_ORDER_DATE = b_query.FIRST_ORDER_DATE and 
                a_query.dbt_audit_helper_pk is not null and b_query.dbt_audit_helper_pk is not null,
            (a_query.FIRST_ORDER_DATE is null and b_query.FIRST_ORDER_DATE is null),
            false
        ) as perfect_match,
        a_query.FIRST_ORDER_DATE is null and a_query.dbt_audit_helper_pk is not null as null_in_a,
        b_query.FIRST_ORDER_DATE is null and b_query.dbt_audit_helper_pk is not null as null_in_b,
        a_query.dbt_audit_helper_pk is null as missing_from_a,
        b_query.dbt_audit_helper_pk is null as missing_from_b,
        coalesce(
            a_query.dbt_audit_helper_pk is not null and b_query.dbt_audit_helper_pk is not null and 
            -- ensure that neither value is missing before considering it a conflict
            (
                a_query.FIRST_ORDER_DATE != b_query.FIRST_ORDER_DATE or -- two not-null values that do not match
                (a_query.FIRST_ORDER_DATE is not null and b_query.FIRST_ORDER_DATE is null) or -- null in b and not null in a
                (a_query.FIRST_ORDER_DATE is null and b_query.FIRST_ORDER_DATE is not null) -- null in a and not null in b
            ), 
            false
        ) as conflicting_values
        -- considered a conflict if the values do not match AND at least one of the values is not null.

    from a_query

    full outer join b_query on (a_query.dbt_audit_helper_pk = b_query.dbt_audit_helper_pk)



 )

    

      union all

    

  

    

    /*  Create a query combining results from all columns so that the user, or the 
    test suite, can examine all at once.
    */
    
    

    /*  There will be one audit_query subquery for each column
    */
    ( with a_query as (
          
    select
      *,
      order_id as dbt_audit_helper_pk
    from ANALYTICS.DBT_JSILV.CUSTOMER_ORDERS_LEGACY
  
),

b_query as (
    
    select
      *,
      order_id as dbt_audit_helper_pk
    from ANALYTICS.DBT_JSILV.fct_customer_orders
  
)
    select
        coalesce(a_query.dbt_audit_helper_pk, b_query.dbt_audit_helper_pk) as primary_key,

        
            'ORDER_COUNT' as column_name,
        

        coalesce(
            a_query.ORDER_COUNT = b_query.ORDER_COUNT and 
                a_query.dbt_audit_helper_pk is not null and b_query.dbt_audit_helper_pk is not null,
            (a_query.ORDER_COUNT is null and b_query.ORDER_COUNT is null),
            false
        ) as perfect_match,
        a_query.ORDER_COUNT is null and a_query.dbt_audit_helper_pk is not null as null_in_a,
        b_query.ORDER_COUNT is null and b_query.dbt_audit_helper_pk is not null as null_in_b,
        a_query.dbt_audit_helper_pk is null as missing_from_a,
        b_query.dbt_audit_helper_pk is null as missing_from_b,
        coalesce(
            a_query.dbt_audit_helper_pk is not null and b_query.dbt_audit_helper_pk is not null and 
            -- ensure that neither value is missing before considering it a conflict
            (
                a_query.ORDER_COUNT != b_query.ORDER_COUNT or -- two not-null values that do not match
                (a_query.ORDER_COUNT is not null and b_query.ORDER_COUNT is null) or -- null in b and not null in a
                (a_query.ORDER_COUNT is null and b_query.ORDER_COUNT is not null) -- null in a and not null in b
            ), 
            false
        ) as conflicting_values
        -- considered a conflict if the values do not match AND at least one of the values is not null.

    from a_query

    full outer join b_query on (a_query.dbt_audit_helper_pk = b_query.dbt_audit_helper_pk)



 )

    

      union all

    

  

    

    /*  Create a query combining results from all columns so that the user, or the 
    test suite, can examine all at once.
    */
    
    

    /*  There will be one audit_query subquery for each column
    */
    ( with a_query as (
          
    select
      *,
      order_id as dbt_audit_helper_pk
    from ANALYTICS.DBT_JSILV.CUSTOMER_ORDERS_LEGACY
  
),

b_query as (
    
    select
      *,
      order_id as dbt_audit_helper_pk
    from ANALYTICS.DBT_JSILV.fct_customer_orders
  
)
    select
        coalesce(a_query.dbt_audit_helper_pk, b_query.dbt_audit_helper_pk) as primary_key,

        
            'TOTAL_LIFETIME_VALUE' as column_name,
        

        coalesce(
            a_query.TOTAL_LIFETIME_VALUE = b_query.TOTAL_LIFETIME_VALUE and 
                a_query.dbt_audit_helper_pk is not null and b_query.dbt_audit_helper_pk is not null,
            (a_query.TOTAL_LIFETIME_VALUE is null and b_query.TOTAL_LIFETIME_VALUE is null),
            false
        ) as perfect_match,
        a_query.TOTAL_LIFETIME_VALUE is null and a_query.dbt_audit_helper_pk is not null as null_in_a,
        b_query.TOTAL_LIFETIME_VALUE is null and b_query.dbt_audit_helper_pk is not null as null_in_b,
        a_query.dbt_audit_helper_pk is null as missing_from_a,
        b_query.dbt_audit_helper_pk is null as missing_from_b,
        coalesce(
            a_query.dbt_audit_helper_pk is not null and b_query.dbt_audit_helper_pk is not null and 
            -- ensure that neither value is missing before considering it a conflict
            (
                a_query.TOTAL_LIFETIME_VALUE != b_query.TOTAL_LIFETIME_VALUE or -- two not-null values that do not match
                (a_query.TOTAL_LIFETIME_VALUE is not null and b_query.TOTAL_LIFETIME_VALUE is null) or -- null in b and not null in a
                (a_query.TOTAL_LIFETIME_VALUE is null and b_query.TOTAL_LIFETIME_VALUE is not null) -- null in a and not null in b
            ), 
            false
        ) as conflicting_values
        -- considered a conflict if the values do not match AND at least one of the values is not null.

    from a_query

    full outer join b_query on (a_query.dbt_audit_helper_pk = b_query.dbt_audit_helper_pk)



 )

    

      union all

    

  

    

    /*  Create a query combining results from all columns so that the user, or the 
    test suite, can examine all at once.
    */
    
    

    /*  There will be one audit_query subquery for each column
    */
    ( with a_query as (
          
    select
      *,
      order_id as dbt_audit_helper_pk
    from ANALYTICS.DBT_JSILV.CUSTOMER_ORDERS_LEGACY
  
),

b_query as (
    
    select
      *,
      order_id as dbt_audit_helper_pk
    from ANALYTICS.DBT_JSILV.fct_customer_orders
  
)
    select
        coalesce(a_query.dbt_audit_helper_pk, b_query.dbt_audit_helper_pk) as primary_key,

        
            'ORDER_VALUE_DOLLARS' as column_name,
        

        coalesce(
            a_query.ORDER_VALUE_DOLLARS = b_query.ORDER_VALUE_DOLLARS and 
                a_query.dbt_audit_helper_pk is not null and b_query.dbt_audit_helper_pk is not null,
            (a_query.ORDER_VALUE_DOLLARS is null and b_query.ORDER_VALUE_DOLLARS is null),
            false
        ) as perfect_match,
        a_query.ORDER_VALUE_DOLLARS is null and a_query.dbt_audit_helper_pk is not null as null_in_a,
        b_query.ORDER_VALUE_DOLLARS is null and b_query.dbt_audit_helper_pk is not null as null_in_b,
        a_query.dbt_audit_helper_pk is null as missing_from_a,
        b_query.dbt_audit_helper_pk is null as missing_from_b,
        coalesce(
            a_query.dbt_audit_helper_pk is not null and b_query.dbt_audit_helper_pk is not null and 
            -- ensure that neither value is missing before considering it a conflict
            (
                a_query.ORDER_VALUE_DOLLARS != b_query.ORDER_VALUE_DOLLARS or -- two not-null values that do not match
                (a_query.ORDER_VALUE_DOLLARS is not null and b_query.ORDER_VALUE_DOLLARS is null) or -- null in b and not null in a
                (a_query.ORDER_VALUE_DOLLARS is null and b_query.ORDER_VALUE_DOLLARS is not null) -- null in a and not null in b
            ), 
            false
        ) as conflicting_values
        -- considered a conflict if the values do not match AND at least one of the values is not null.

    from a_query

    full outer join b_query on (a_query.dbt_audit_helper_pk = b_query.dbt_audit_helper_pk)



 )

    

      union all

    

  

    

    /*  Create a query combining results from all columns so that the user, or the 
    test suite, can examine all at once.
    */
    
    

    /*  There will be one audit_query subquery for each column
    */
    ( with a_query as (
          
    select
      *,
      order_id as dbt_audit_helper_pk
    from ANALYTICS.DBT_JSILV.CUSTOMER_ORDERS_LEGACY
  
),

b_query as (
    
    select
      *,
      order_id as dbt_audit_helper_pk
    from ANALYTICS.DBT_JSILV.fct_customer_orders
  
)
    select
        coalesce(a_query.dbt_audit_helper_pk, b_query.dbt_audit_helper_pk) as primary_key,

        
            'ORDER_STATUS' as column_name,
        

        coalesce(
            a_query.ORDER_STATUS = b_query.ORDER_STATUS and 
                a_query.dbt_audit_helper_pk is not null and b_query.dbt_audit_helper_pk is not null,
            (a_query.ORDER_STATUS is null and b_query.ORDER_STATUS is null),
            false
        ) as perfect_match,
        a_query.ORDER_STATUS is null and a_query.dbt_audit_helper_pk is not null as null_in_a,
        b_query.ORDER_STATUS is null and b_query.dbt_audit_helper_pk is not null as null_in_b,
        a_query.dbt_audit_helper_pk is null as missing_from_a,
        b_query.dbt_audit_helper_pk is null as missing_from_b,
        coalesce(
            a_query.dbt_audit_helper_pk is not null and b_query.dbt_audit_helper_pk is not null and 
            -- ensure that neither value is missing before considering it a conflict
            (
                a_query.ORDER_STATUS != b_query.ORDER_STATUS or -- two not-null values that do not match
                (a_query.ORDER_STATUS is not null and b_query.ORDER_STATUS is null) or -- null in b and not null in a
                (a_query.ORDER_STATUS is null and b_query.ORDER_STATUS is not null) -- null in a and not null in b
            ), 
            false
        ) as conflicting_values
        -- considered a conflict if the values do not match AND at least one of the values is not null.

    from a_query

    full outer join b_query on (a_query.dbt_audit_helper_pk = b_query.dbt_audit_helper_pk)



 )

    

      union all

    

  

    

    /*  Create a query combining results from all columns so that the user, or the 
    test suite, can examine all at once.
    */
    
    

    /*  There will be one audit_query subquery for each column
    */
    ( with a_query as (
          
    select
      *,
      order_id as dbt_audit_helper_pk
    from ANALYTICS.DBT_JSILV.CUSTOMER_ORDERS_LEGACY
  
),

b_query as (
    
    select
      *,
      order_id as dbt_audit_helper_pk
    from ANALYTICS.DBT_JSILV.fct_customer_orders
  
)
    select
        coalesce(a_query.dbt_audit_helper_pk, b_query.dbt_audit_helper_pk) as primary_key,

        
            'PAYMENT_STATUS' as column_name,
        

        coalesce(
            a_query.PAYMENT_STATUS = b_query.PAYMENT_STATUS and 
                a_query.dbt_audit_helper_pk is not null and b_query.dbt_audit_helper_pk is not null,
            (a_query.PAYMENT_STATUS is null and b_query.PAYMENT_STATUS is null),
            false
        ) as perfect_match,
        a_query.PAYMENT_STATUS is null and a_query.dbt_audit_helper_pk is not null as null_in_a,
        b_query.PAYMENT_STATUS is null and b_query.dbt_audit_helper_pk is not null as null_in_b,
        a_query.dbt_audit_helper_pk is null as missing_from_a,
        b_query.dbt_audit_helper_pk is null as missing_from_b,
        coalesce(
            a_query.dbt_audit_helper_pk is not null and b_query.dbt_audit_helper_pk is not null and 
            -- ensure that neither value is missing before considering it a conflict
            (
                a_query.PAYMENT_STATUS != b_query.PAYMENT_STATUS or -- two not-null values that do not match
                (a_query.PAYMENT_STATUS is not null and b_query.PAYMENT_STATUS is null) or -- null in b and not null in a
                (a_query.PAYMENT_STATUS is null and b_query.PAYMENT_STATUS is not null) -- null in a and not null in b
            ), 
            false
        ) as conflicting_values
        -- considered a conflict if the values do not match AND at least one of the values is not null.

    from a_query

    full outer join b_query on (a_query.dbt_audit_helper_pk = b_query.dbt_audit_helper_pk)



 )

    

    ),

        final as (
          select
            upper(column_name) as column_name,
            sum(case when perfect_match then 1 else 0 end) as perfect_match,
            sum(case when null_in_a then 1 else 0 end) as null_in_a,
            sum(case when null_in_b then 1 else 0 end) as null_in_b,
            sum(case when missing_from_a then 1 else 0 end) as missing_from_a,
            sum(case when missing_from_b then 1 else 0 end) as missing_from_b,
            sum(case when conflicting_values then 1 else 0 end) as conflicting_values
          from main
          group by 1
          order by column_name
        )

    --   select * from final
    -- select * from main 
    -- where column_name = 'ORDER_VALUE_DOLLARS' and perfect_match = false
    
    

  with legacy as (
    select 
    order_id,
    order_value_dollars
    from {{ref('customer_orders_legacy')}}
  ),
  refactored as(
    select 
    order_id,
    order_value_dollars
    from {{ref('fct_customer_orders')}}
  )
    
select 
    coalesce(legacy.order_id, refactored.order_id) as order_id,
    coalesce(legacy.order_value_dollars, refactored.order_value_dollars) as order_value_dollars,
    (legacy.order_id is not null) as in_legacy,
    (refactored.order_id is not null) as in_refactored
from legacy
full outer join refactored 
on (legacy.order_id = refactored.order_id) 
and legacy.order_value_dollars = refactored.order_value_dollars
where  in_legacy != in_refactored
order by order_id

) as __preview_sbq__ limit 1000