ATTACH TABLE _ UUID '68470a75-7bd9-463b-9fe1-4af7ec4d708b'
(
    `t.year` UInt16,
    `month_name` String,
    `total_inventory_value` Float64,
    `inventory_turnover_ratio` Nullable(Float64),
    `age_range` String,
    `inventory_value` Float64
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS replicated_deduplication_window = 0, index_granularity = 8192
