ATTACH TABLE _ UUID '68b53333-5528-47ec-a8ca-d67937c5b705'
(
    `year` Int64,
    `month_name` String,
    `total_sales` Float64,
    `total_profit` Float64,
    `avg_profit_margin` Float64,
    `avg_actual_shipment_days` Float64,
    `avg_scheduled_shipment_days` Float64
)
ENGINE = MergeTree
ORDER BY (year, month_name)
SETTINGS index_granularity = 8192
