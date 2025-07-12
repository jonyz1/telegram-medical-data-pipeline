
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Custom test to ensure no duplicate messages per channel and date
SELECT
    message_id,
    channel_name,
    message_date::DATE,
    COUNT(*) AS cnt
FROM "hello"."public_staging"."stg_telegram_messages"
GROUP BY message_id, channel_name, message_date::DATE
HAVING COUNT(*) > 1
  
  
      
    ) dbt_internal_test