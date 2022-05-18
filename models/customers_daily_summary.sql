select

    customer_id,
    order_date,
    count(*)

from {{ ref('stg_orders') }}
group by 1, 2