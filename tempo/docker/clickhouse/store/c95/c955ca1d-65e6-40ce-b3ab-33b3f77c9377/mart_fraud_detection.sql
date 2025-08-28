ATTACH TABLE _ UUID '21b877e0-6eb4-4d93-beab-8e604f54f8fd'
(
    `order_customer_id` Nullable(Int64),
    `avg_order_total` Nullable(Float64),
    `num_orders` Nullable(Int64)
)
ENGINE = Memory
