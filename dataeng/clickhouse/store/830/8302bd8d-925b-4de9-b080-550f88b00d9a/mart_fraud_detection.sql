ATTACH TABLE _ UUID '82301efc-0a6c-4497-9ea2-fcc98f8d2790'
(
    `order_customer_id` String,
    `avg_order_total` Float64,
    `num_orders` UInt64
)
ENGINE = MergeTree
ORDER BY order_customer_id
SETTINGS replicated_deduplication_window = 0, index_granularity = 8192
