{{ config(  
    materialized='incremental',  
    unique_key='gameId'  
) }}  

{% set chunk_days = 30 %}  -- every 30 days

-- First and last date
WITH date_ranges AS (  
    SELECT  
        MIN(createdAt) AS earliest_date,  
        MAX(createdAt) AS latest_date  
    FROM `bigquery-public-data.baseball.games_wide`  
),  

-- Condition
source_data AS (  
    SELECT *  
    FROM `bigquery-public-data.baseball.games_wide`  
    WHERE   
        {% if is_incremental() %}  
            createdAt > (SELECT MAX(createdAt) FROM {{ this }})  -- incremental - only new data.
        {% else %}  
            createdAt BETWEEN DATE_SUB((SELECT MAX(latest_date) FROM date_ranges), INTERVAL {{ chunk_days }} DAY)   
                           AND (SELECT MAX(latest_date) FROM date_ranges)  -- Full data by every 30 days 
        {% endif %}  
)  

-- main data
SELECT  
    gameId,  
    seasonId,  
    seasonType,  
    startTime,  
    gameStatus,  
    attendance,  
    dayNight,  
    duration,  
    durationMinutes,  
    createdAt,  
    updatedAt  
FROM source_data  
ORDER BY createdAt DESC