{{ 
    config(
        materialized='table',
        schema='gold'
    ) 
}}

with base as (

    select *
    from {{ ref('orders') }}

),

aggregated as (

    select
        order_year,
        order_month,

        count(*)                                    as total_orders,
        sum(total_amount)                            as total_revenue,
        avg(total_amount)                            as avg_ticket,

        countIf(status = 'cancelled')                as cancelled_orders,
        countIf(status = 'delivered')                as delivered_orders,

        countIf(is_fulfilled = 1)                    as fulfilled_orders

    from base
    group by
        order_year,
        order_month

)

select * from aggregated