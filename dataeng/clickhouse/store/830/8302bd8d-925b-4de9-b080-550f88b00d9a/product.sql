ATTACH TABLE _ UUID 'e0b18590-7183-46e4-b92d-75219d2f3e8f'
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
