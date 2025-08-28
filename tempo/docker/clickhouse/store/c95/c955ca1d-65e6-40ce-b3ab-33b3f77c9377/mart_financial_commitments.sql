ATTACH TABLE _ UUID 'ddd32ade-56f7-4c54-9617-8782d2336b90'
(
    `Department_Name` Nullable(String),
    `Market` Nullable(String),
    `category_name` Nullable(String),
    `total_committed_funds` Nullable(Float64),
    `commitment_fulfillment_rate` Nullable(Float64)
)
ENGINE = Memory
