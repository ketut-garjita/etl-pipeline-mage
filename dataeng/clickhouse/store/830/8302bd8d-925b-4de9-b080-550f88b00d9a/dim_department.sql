ATTACH TABLE _ UUID '18d89899-1b16-427a-8429-6f361c54d34e'
(
    `department_id` String,
    `department_name` String,
    `market` String
)
ENGINE = MergeTree
ORDER BY department_id
SETTINGS index_granularity = 8192
