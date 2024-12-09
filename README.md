# Custom Incremental Model  

## Description  

The `custom_incremental_model.sql` model is designed to extract and process baseball game data from the public BigQuery dataset. 
It utilizes incremental data loading, which allows for the update of only new records, as well as the extraction of full data every 30 days.

### Difference from Standard Incremental Model  

Unlike a standard incremental model, which typically only retrieves new records based on the `createdAt` timestamp, this model incorporates a more sophisticated approach by defining a date range.
This standard model (incremental_model.sql) only considers the most recent records based on the createdAt timestamp. In contrast, the custom_incremental_model.sql uses a defined chunk of days (30 days) to manage data extraction more effectively, allowing for both incremental updates and full data retrieval at specified intervals. This ensures that the model captures a broader set of data while still optimizing performance.

## Configuration  

```sql
{{ config(  
    materialized='incremental',  
    unique_key='gameId'  
) }}
```
- materialized: Defines how the model is materialized. In this case, it uses incremental loading.
- unique_key: A unique identifier for records, which is gameId.

## Parameters  
```markdown 
- *chunk_days*: This parameter defines the time interval for incremental loading. Here, it is set to 30 days.
```

## Model Logic  

### 1. Define the First and Last Dates  

The model first determines the date range using the following query:  
```sql  
WITH date_ranges AS (  
    SELECT  
        MIN(createdAt) AS earliest_date,  
        MAX(createdAt) AS latest_date  
    FROM `bigquery-public-data.baseball.games_wide`  
)
```
### 2. Condition for Data Extraction

Depending on whether the request is incremental, the model selects the data:
```sql  
source_data AS (  
    SELECT *  
    FROM `bigquery-public-data.baseball.games_wide`  
    WHERE   
        {% if is_incremental() %}  
            createdAt > (SELECT MAX(createdAt) FROM {{ this }})  -- incremental - only new data.  
        {% else %}  
            createdAt BETWEEN DATE_SUB((SELECT MAX(latest_date) FROM date_ranges), INTERVAL {{ chunk_days }} DAY)   
                           AND (SELECT MAX(latest_date) FROM date_ranges)  -- Full data every 30 days   
        {% endif %}  
)
```

### 3. Main Query

The main query extracts the necessary fields from the table:

```sql  
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
```


## Usage  

1. Copy the `custom_incremental_model.sql` file to your working directory.  
2. Ensure you have access to the dataset `bigquery-public-data.baseball.games_wide`.  
3. Run the model in your supported SQL environment.


## Conclusion  

This model allows for effective management of baseball game data by minimizing the volume of data extracted and optimizing query performance.
