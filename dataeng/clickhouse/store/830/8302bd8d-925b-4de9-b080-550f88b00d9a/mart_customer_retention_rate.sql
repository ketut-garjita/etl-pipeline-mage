ATTACH TABLE _ UUID '0b0cd941-61a6-4234-9ff0-c70f8e5e54ed'
(
    `year` UInt16,
    `month_name` String,
    `distinct_customers` UInt64,
    `rolling_customers` UInt64,
    `customer_retention_rate` Float64
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS replicated_deduplication_window = 0, index_granularity = 8192
