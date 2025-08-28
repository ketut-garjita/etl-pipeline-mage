ATTACH TABLE _ UUID '3bff0fb0-0637-46e0-8721-251bf8941d07'
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
