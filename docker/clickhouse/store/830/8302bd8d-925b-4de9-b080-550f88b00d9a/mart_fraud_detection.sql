ATTACH TABLE _ UUID '4af8a9df-8d6e-412d-8ec5-1ca9b2cfbf0c'
(
    `order_customer_id` Nullable(Int64),
    `avg_order_total` Nullable(Float64),
    `num_orders` Nullable(Int64)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 8192
