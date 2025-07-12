
  
    

  create  table "hello"."public_marts"."dim_channels__dbt_tmp"
  
  
    as
  
  (
    

-- Dimension table for Telegram channels
WITH source AS (
    SELECT DISTINCT channel_name
    FROM "hello"."public_staging"."stg_telegram_messages"
)
SELECT
    ROW_NUMBER() OVER () AS channel_id,
    channel_name
FROM source
  );
  