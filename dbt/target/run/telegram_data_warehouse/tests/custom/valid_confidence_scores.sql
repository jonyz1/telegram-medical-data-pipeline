
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Custom test to ensure confidence scores are between 0 and 1
SELECT
    message_id,
    detected_object_class,
    confidence_score
FROM "hello"."public_staging"."stg_image_detections"
WHERE confidence_score < 0 OR confidence_score > 1
  
  
      
    ) dbt_internal_test