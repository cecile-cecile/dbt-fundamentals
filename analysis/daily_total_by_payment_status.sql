with payments as (

    select * from {{ ref('stg_payments') }}

),

daily_total as (

    select
        created_at,
        sum(amount) as total_possible_revenue,
        
        {%- for status in ['success', 'fail'] -%}
        sum(case when status = '{{ status }}' then amount else 0 end) as {{ status }}_total{{ ',' if not loop.last}}
        {% endfor %}
    from payments
    group by 1
    order by 1

)

select * from daily_total