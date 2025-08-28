ATTACH TABLE _ UUID 'd176468d-5d9a-4109-9d86-e1e2d57c9b1b'
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
