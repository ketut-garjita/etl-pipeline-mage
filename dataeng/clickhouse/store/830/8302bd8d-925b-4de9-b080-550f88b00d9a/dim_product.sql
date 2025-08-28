ATTACH TABLE _ UUID 'cfa990f2-64bf-408d-b45c-af2a54fd0b7f'
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
