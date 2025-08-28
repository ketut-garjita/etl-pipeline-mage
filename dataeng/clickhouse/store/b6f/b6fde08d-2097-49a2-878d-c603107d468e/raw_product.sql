ATTACH TABLE _ UUID '3e31593f-d738-4243-b1f8-978d16af164c'
(
    `product_card_id` String,
    `product_category_id` String,
    `category_name` String,
    `product_description` String,
    `product_image` String,
    `product_name` String,
    `product_price` String,
    `product_status` String
)
ENGINE = MergeTree
ORDER BY product_card_id
SETTINGS index_granularity = 8192
