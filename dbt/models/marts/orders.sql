{{
    config(
        materialized='table',
        schema='silver',
        engine='MergeTree()',
        order_by='order_id'
    )
}}

/*
    orders — Modelo final Silver, pronto para análise.

    Responsabilidades:
      - Nomes de colunas padronizados e orientados ao negócio
      - Colunas derivadas de análise (is_cancelled, order_year, etc.)
      - Sem colunas técnicas de pipeline (batch_id, raw_path, source removidos)
      - ingestion_timestamp mantido apenas para auditoria de dados
*/

with staged as (

    select * from {{ ref('stg_orders') }}

),

final as (

    select
        -- Identificadores
        order_id,
        customer_id,

        -- Negócio
        status,
        total_amount,

        -- Colunas derivadas
        status = 'cancelled'                        as is_cancelled,
        status in ('shipped', 'delivered')          as is_fulfilled,
        toDate(created_at)                          as order_date,
        toYear(created_at)                          as order_year,
        toMonth(created_at)                         as order_month,

        -- Timestamps de negócio
        created_at,
        updated_at,

        -- Auditoria (mantido para rastreabilidade mínima sem expor internos do pipeline)
        ingestion_timestamp

    from staged

)

select * from final
