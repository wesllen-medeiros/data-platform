{{
    config(
        materialized='view',
        schema='silver_staging'
    )
}}

/*
    stg_orders — Staging da camada Silver para pedidos.

    Responsabilidades:
      - Cast de tipos para garantir consistência
      - Remoção de registros com campos obrigatórios nulos
      - Padronização de nomes de colunas
      - Normalização de valores categóricos (status)
      - Manutenção das colunas técnicas para rastreabilidade
*/

with source as (

    select * from {{ source('bronze', 'orders') }}

),

casted as (

    select
        -- Identificadores
        toInt64(order_id)                                   as order_id,
        toInt64(customer_id)                                as customer_id,

        -- Financeiro
        toFloat64(coalesce(total_amount, 0))                as total_amount,

        -- Categórico normalizado para minúsculas e sem espaços extras
        lower(trim(status))                                 as status,

        -- Timestamps com cast explícito
        toDateTimeOrNull(created_at)                        as created_at,
        toDateTimeOrNull(updated_at)                        as updated_at,

        -- Colunas técnicas de lineage (mantidas para rastreabilidade)
        ingestion_timestamp,
        batch_id,
        raw_path,
        source

    from source

),

filtered as (

    select *
    from casted
    where
        -- Campos obrigatórios não podem ser nulos
        order_id    is not null
        and customer_id is not null
        and created_at  is not null

        -- Valores negativos não são válidos para pedidos
        and (total_amount is null or total_amount >= 0)

        -- Status dentro dos valores conhecidos
        and status in ('pending', 'confirmed', 'shipped', 'delivered', 'cancelled')

)

select * from filtered
