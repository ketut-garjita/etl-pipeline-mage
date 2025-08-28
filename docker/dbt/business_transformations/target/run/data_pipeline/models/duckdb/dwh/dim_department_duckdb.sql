
  
  create view "warehouse"."main"."dim_department_duckdb__dbt_tmp" as (
    

WITH cleaned AS (
SELECT 
    Department_Id,
    Department_Name,
    Market,
    Ingestion_Time
FROM "warehouse"."main"."stg_department_duckdb"
WHERE Department_Id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY Department_Id, Department_Name, Market) = 1
)

SELECT * FROM cleaned
ORDER BY Department_Id
  );
