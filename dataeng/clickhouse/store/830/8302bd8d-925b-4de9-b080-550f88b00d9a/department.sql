ATTACH TABLE _ UUID '65474ef3-0fc2-4c46-9263-f35c27567e7e'
(
    `department_id` String,
    `department_name` String,
    `market` String
)
ENGINE = MergeTree
ORDER BY department_id
SETTINGS index_granularity = 8192
