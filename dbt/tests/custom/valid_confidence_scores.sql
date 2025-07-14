-- Custom test to ensure confidence scores are between 0 and 1
SELECT
    message_id,
    detected_object_class,
    confidence_score
FROM {{ ref('stg_image_detections') }}
WHERE confidence_score < 0 OR confidence_score > 1