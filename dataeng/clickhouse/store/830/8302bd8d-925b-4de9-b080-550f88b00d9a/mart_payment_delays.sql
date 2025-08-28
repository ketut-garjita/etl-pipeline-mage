ATTACH TABLE _ UUID '75cce6d3-43c7-4042-b23e-53e2d2d60862'
(
    `delivery_status` String,
    `avg_payment_delay` Float64,
    `count_of_orders` UInt64
)
ENGINE = MergeTree
PARTITION BY delivery_status
ORDER BY tuple()
SETTINGS replicated_deduplication_window = 0, index_granularity = 8192
