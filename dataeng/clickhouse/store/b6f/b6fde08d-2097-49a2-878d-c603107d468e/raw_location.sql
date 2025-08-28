ATTACH TABLE _ UUID '3e227907-a028-4da0-a2bf-8e2d3db9fa56'
(
    `order_zipcode` String,
    `order_city` String,
    `order_state` String,
    `order_region` String,
    `order_country` String,
    `latitude` String,
    `longitude` String
)
ENGINE = MergeTree
ORDER BY order_zipcode
SETTINGS index_granularity = 8192
