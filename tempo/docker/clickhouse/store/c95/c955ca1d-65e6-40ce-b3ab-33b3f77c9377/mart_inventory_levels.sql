ATTACH TABLE _ UUID '74b3a508-aa4d-4ece-bbb2-c0e9096808fa'
(
    `year` Nullable(Int64),
    `month_name` Nullable(String),
    `total_inventory_value` Nullable(Float64),
    `inventory_turnover_ratio` Nullable(Float64),
    `age_range` Nullable(String),
    `inventory_value` Nullable(Float64)
)
ENGINE = Memory
