select
    status,
    sum(amount) as totals
from {{ ref('stg_stripe_payments') }}
where status = 'success'
group by 1