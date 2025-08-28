{{ config(materialized='view') }}

WITH cleaned AS (
SELECT 
    Department_Id,
    Department_Name,
    Market,
    Ingestion_Time
FROM {{ ref('stg_department') }}
WHERE Department_Id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY Department_Id, Department_Name, Market) = 1
)

SELECT * FROM cleaned
ORDER BY Department_Id 

