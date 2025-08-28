ATTACH TABLE _ UUID '658587a0-8c8c-46cb-b551-e2a418dfb10c'
(
    `customer_id` String,
    `customer_email` String,
    `customer_fname` String,
    `customer_lname` String,
    `customer_segment` String,
    `customer_city` String,
    `customer_country` String,
    `customer_state` String,
    `customer_street` String,
    `customer_zipcode` String
)
ENGINE = MergeTree
ORDER BY customer_id
SETTINGS index_granularity = 8192
