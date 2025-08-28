ATTACH TABLE _ UUID 'e2e3e6f8-b79f-481f-992c-75b46475727d'
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
