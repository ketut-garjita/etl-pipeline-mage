ATTACH TABLE _ UUID 'f16627e0-3b15-47d4-b9d5-42a3bd06b347'
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
