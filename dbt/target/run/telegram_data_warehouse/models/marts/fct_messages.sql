
  
    

  create  table "hello"."public_public_marts"."fct_messages__dbt_tmp"
  
  
    as
  
  (
    -- 

-- -- Fact table for Telegram messages, linking to dim_channels and dim_dates
-- -- Contains metrics like message_length and supports analysis of visual content and trends
-- WITH messages AS (
--     SELECT
--         m.message_id,
--         m.channel_name,
--         m.message_date,
--         m.message_text,
--         m.sender_id,
--         m.has_media,
--         m.media_type,
--         m.media_path,
--         m.caption,
--         m.is_reply,
--         m.forwarded_from,
--         LENGTH(COALESCE(m.message_text, '')) AS message_length,
--         c.channel_id,
--         (EXTRACT(EPOCH FROM m.message_date)::BIGINT * 1000) AS date_id
--     FROM "hello"."public_staging"."stg_telegram_messages" m
--     LEFT JOIN "hello"."public_public_marts"."dim_channels" c
--         ON m.channel_name = c.channel_name
-- )
-- SELECT
--     (channel_id || '-' || message_id || '-' || date_id)::TEXT AS message_key,
--     channel_id,
--     date_id,
--     message_id,
--     message_text,
--     sender_id,
--     has_media,
--     media_type,
--     media_path,
--     caption,
--     is_reply,
--     forwarded_from,
--     message_length
-- FROM messages




-- Fact table for Telegram messages, linking to dim_channels and dim_dates
WITH messages AS (
    SELECT
        m.message_id,
        m.channel_name,
        m.message_date,
        m.message_text,
        m.sender_id,
        m.has_media,
        m.media_type,
        m.media_path,
        m.caption,
        m.is_reply,
        m.forwarded_from,
        LENGTH(COALESCE(m.message_text, '')) AS message_length,
        c.channel_id,
        (EXTRACT(EPOCH FROM m.message_date)::BIGINT * 1000) AS date_id
    FROM "hello"."public_staging"."stg_telegram_messages" m
    LEFT JOIN "hello"."public_public_marts"."dim_channels" c
        ON m.channel_name = c.channel_name
)
SELECT
    (COALESCE(channel_id::TEXT, 'unknown') || '-' || message_id || '-' || date_id)::TEXT AS message_key,
    channel_id,
    date_id,
    message_id,
    message_text,
    sender_id,
    has_media,
    media_type,
    media_path,
    caption,
    is_reply,
    forwarded_from,
    message_length
FROM messages
  );
  