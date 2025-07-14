

-- Staging model to clean raw image detection data
SELECT
    message_id,
    channel_name,
    detected_object_class,
    confidence_score,
    processed_at
FROM raw.image_detections
WHERE confidence_score BETWEEN 0 AND 1  -- Ensure valid confidence scores