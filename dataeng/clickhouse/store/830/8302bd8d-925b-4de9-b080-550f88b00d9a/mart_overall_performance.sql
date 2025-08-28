ATTACH TABLE _ UUID '2bc9af48-700c-4758-80a2-a0c3e362d5cd'
(
    `os.year` UInt16,
    `os.month` UInt8,
    `month_name` String,
    `total_sales` Float64,
    `total_profit` Float64,
    `avg_profit_margin` Float64,
    `avg_actual_shipment_days` Float64,
    `avg_scheduled_shipment_days` Float64
)
ENGINE = MergeTree
ORDER BY (os.year, os.month)
SETTINGS replicated_deduplication_window = 0, index_granularity = 8192
