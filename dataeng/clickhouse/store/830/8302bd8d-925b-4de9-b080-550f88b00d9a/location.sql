ATTACH TABLE _ UUID 'e16ce0d8-78a0-463e-a966-c65ca055ce2a'
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
ORDER BY latitude
SETTINGS index_granularity = 8192
