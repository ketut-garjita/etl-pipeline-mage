ATTACH TABLE _ UUID '828e4f87-15fc-4249-8dc1-33fbadaee3d7'
(
    `Department_Name` Nullable(String),
    `Market` Nullable(String),
    `category_name` Nullable(String),
    `total_committed_funds` Nullable(Float64),
    `commitment_fulfillment_rate` Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 8192
