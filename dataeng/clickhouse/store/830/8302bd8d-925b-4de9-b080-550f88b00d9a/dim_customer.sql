ATTACH TABLE _ UUID 'def6e54e-2b82-4180-97ec-fc40034ecb87'
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
