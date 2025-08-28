ATTACH TABLE _ UUID '2f182a54-90b1-47dd-9abe-37276df62804'
(
    `delivery_status` Nullable(String),
    `avg_payment_delay` Nullable(Float64),
    `count_of_orders` Nullable(Int64)
)
ENGINE = Memory
