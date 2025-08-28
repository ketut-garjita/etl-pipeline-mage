ATTACH TABLE _ UUID 'c566173d-a254-46dd-8ad8-ce425f1ff5f9'
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
