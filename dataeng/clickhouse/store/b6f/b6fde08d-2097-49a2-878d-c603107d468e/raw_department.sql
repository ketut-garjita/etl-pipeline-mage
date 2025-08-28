ATTACH TABLE _ UUID '01112ee9-db71-4fb5-96be-2836becd2237'
(
    `department_id` String,
    `department_name` String,
    `market` String
)
ENGINE = MergeTree
ORDER BY department_id
SETTINGS index_granularity = 8192
