ATTACH TABLE _ UUID 'b2ec076a-b9c2-49c0-8e53-65c162e30c10'
(
    `department_name` String,
    `market` String,
    `category_name` String,
    `total_committed_funds` Float64,
    `commitment_fulfillment_rate` Float64
)
ENGINE = MergeTree
PARTITION BY department_name
ORDER BY (department_name, market, category_name)
SETTINGS replicated_deduplication_window = 0, index_granularity = 8192
