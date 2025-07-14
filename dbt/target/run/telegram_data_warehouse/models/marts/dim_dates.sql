
  
    

  create  table "hello"."public_public_marts"."dim_dates__dbt_tmp"
  
  
    as
  
  (
    -- 

-- -- Dimension table for time-based analysis
-- WITH date_range AS (
--     SELECT generate_series(
--         '2025-01-01'::DATE,
--         '2025-12-31'::DATE,
--         INTERVAL '1 day'
--     ) AS date_day
-- )
-- SELECT
--     (EXTRACT(EPOCH FROM date_day)::BIGINT * 1000) AS date_id,
--     date_day,
--     EXTRACT(YEAR FROM date_day) AS year,
--     EXTRACT(MONTH FROM date_day) AS month,
--     EXTRACT(DAY FROM date_day) AS day,
--     EXTRACT(DOW FROM date_day) AS day_of_week,
--     EXTRACT(WEEK FROM date_day) AS week_of_year
-- FROM date_range



-- Dimension table for time-based analysis
WITH date_range AS (
    SELECT generate_series(
        '2025-01-01'::TIMESTAMP WITH TIME ZONE,
        '2025-12-31'::TIMESTAMP WITH TIME ZONE,
        INTERVAL '1 day'
    ) AS date_day
)
SELECT
    (EXTRACT(EPOCH FROM date_day)::BIGINT * 1000) AS date_id,
    date_day,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(MONTH FROM date_day) AS month,
    EXTRACT(DAY FROM date_day) AS day,
    EXTRACT(DOW FROM date_day) AS day_of_week,
    EXTRACT(WEEK FROM date_day) AS week_of_year
FROM date_range
  );
  