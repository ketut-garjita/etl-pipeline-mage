ATTACH TABLE _ UUID 'fc4c74da-4c07-40ab-826b-77f3575a3231'
(
    `department_id` String,
    `department_name` String,
    `market` String
)
ENGINE = MergeTree
ORDER BY department_id
SETTINGS index_granularity = 8192
