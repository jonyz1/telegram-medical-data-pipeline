{{ config(materialized='view', schema='staging') }}

-- Staging model to clean and extract fields from raw JSON data
SELECT
    id AS message_id,
    channel_name,
    message_date::TIMESTAMP AS message_date,
    (message_data->>'text')::TEXT AS message_text,
    (message_data->>'sender_id')::BIGINT AS sender_id,
    (message_data->>'has_media')::BOOLEAN AS has_media,
    (message_data->>'media_type')::TEXT AS media_type,
    (message_data->>'media_path')::TEXT AS media_path,
    (message_data->>'caption')::TEXT AS caption,
    (message_data->>'is_reply')::BOOLEAN AS is_reply,
    (message_data->>'forwarded_from')::BIGINT AS forwarded_from,
    loaded_at
FROM raw.telegram_messages