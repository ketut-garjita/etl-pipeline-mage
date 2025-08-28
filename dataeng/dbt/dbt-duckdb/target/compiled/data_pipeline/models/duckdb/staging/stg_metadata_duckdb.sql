

SELECT 
    "key" AS key,
    CAST("offset" AS INTEGER) AS metadata_offset,
    CAST("partition" AS INTEGER) AS partition,
    CAST("time" AS BIGINT) AS time,
    "topic" AS topic,
    CAST("Ingestion_Time" AS TIMESTAMP) AS ingestion_time
FROM "warehouse"."main"."raw_metadata"
--WHERE "key" IS NOT NULL