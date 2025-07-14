-- {{ config(materialized='table', schema='marts', unique_key='detection_key') }}

-- -- Fact table for image detections, linking to fct_messages and dim_channels
-- WITH detections AS (
--     SELECT
--         d.message_id,
--         d.channel_name,
--         d.detected_object_class,
--         d.confidence_score,
--         d.processed_at,
--         c.channel_id
--     FROM {{ ref('stg_image_detections') }} d
--     JOIN {{ ref('dim_channels') }} c
--         ON d.channel_name = c.channel_name
-- )
-- SELECT
--     (d.channel_id || '-' || d.message_id || '-' || d.detected_object_class || '-' || d.processed_at)::TEXT AS detection_key,
--     d.channel_id,
--     d.message_id,
--     d.detected_object_class,
--     d.confidence_score,
--     d.processed_at
-- FROM detections d
-- JOIN {{ ref('fct_messages') }} m
--     ON d.message_id = m.message_id
--     AND d.channel_id = m.channel_id


{{ config(materialized='table', schema='public_marts', unique_key='detection_key') }}

-- Fact table for image detections, linking to fct_messages and dim_channels
WITH detections AS (
    SELECT
        d.message_id,
        d.channel_name,
        d.detected_object_class,
        d.confidence_score,
        d.processed_at,
        c.channel_id
    FROM {{ ref('stg_image_detections') }} d
    JOIN {{ ref('dim_channels') }} c
        ON d.channel_name = c.channel_name
)
SELECT
    (d.channel_id || '-' || d.message_id || '-' || d.detected_object_class || '-' || d.processed_at)::TEXT AS detection_key,
    d.channel_id,
    d.message_id,
    d.detected_object_class,
    d.confidence_score,
    d.processed_at
FROM detections d
JOIN {{ ref('fct_messages') }} m
    ON d.message_id = m.message_id
    AND d.channel_id = m.channel_id