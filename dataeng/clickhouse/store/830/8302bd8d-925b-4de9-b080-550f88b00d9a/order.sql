ATTACH TABLE _ UUID 'bc923510-d371-4184-a68c-5a7dfea44598'
(
    `order_id` String,
    `order_date` String,
    `order_customer_id` String,
    `order_item_id` String,
    `product_card_id` String,
    `order_item_discount` Float64,
    `order_item_discount_rate` Float64,
    `order_item_product_price` Float64,
    `order_item_profit_ratio` String,
    `order_item_quantity` Int32,
    `sales_per_customer` Float64,
    `sales` Float64,
    `order_item_total` Float64,
    `order_profit_per_order` Float64,
    `order_status` String,
    `department_id` Int32
)
ENGINE = MergeTree
ORDER BY order_id
SETTINGS index_granularity = 8192
