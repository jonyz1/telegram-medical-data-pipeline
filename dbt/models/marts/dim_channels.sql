{{ config(materialized='table', schema='marts', unique_key='channel_id') }}

-- Dimension table for Telegram channels
WITH source AS (
    SELECT DISTINCT channel_name
    FROM {{ ref('stg_telegram_messages') }}
)
SELECT
    ROW_NUMBER() OVER () AS channel_id,
    channel_name
FROM source