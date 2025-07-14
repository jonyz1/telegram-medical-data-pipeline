
  
    

  create  table "hello"."public_marts"."fct_image_detections__dbt_tmp"
  
  
    as
  
  (
    

-- Fact table for image detections, linking to fct_messages and dim_channels
WITH detections AS (
    SELECT
        d.message_id,
        d.channel_name,
        d.detected_object_class,
        d.confidence_score,
        d.processed_at,
        c.channel_id
    FROM "hello"."public_staging"."stg_image_detections" d
    JOIN "hello"."public_marts"."dim_channels" c
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
JOIN "hello"."public_marts"."fct_messages" m
    ON d.message_id = m.message_id
    AND d.channel_id = m.channel_id
  );
  