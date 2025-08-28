

SELECT 
    CAST("Department Id" AS INTEGER) AS Department_Id,
    "Department Name" AS Department_Name,
    "Market" AS Market,
    CAST("Ingestion_Time" AS TIMESTAMP) AS ingestion_time
FROM `main`.`raw_department`
--WHERE "Department Id" IS NOT NULL