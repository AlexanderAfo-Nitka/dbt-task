{{ config(  
    materialized='incremental',  
    unique_key='gameId'           
) }}  

WITH source_data AS (  
    SELECT *  
    FROM `bigquery-public-data.baseball.games_wide`  
    {% if is_incremental() %}  
        WHERE createdAt > (SELECT MAX(createdAt) FROM {{ this }})  
    {% endif %}  
)  

SELECT  
    gameId,  
    seasonId,  
    seasonType,  
    year,  
    startTime,  
    gameStatus,  
    attendance,  
    dayNight,  
    duration,  
    durationMinutes,  
    awayTeamName,  
    homeTeamName,  
    venueName,  
    venueCapacity,  
    venueCity,  
    venueMarket,  
    homeFinalRuns,  
    homeFinalHits,  
    description,  
    createdAt,  
    updatedAt  
FROM source_data