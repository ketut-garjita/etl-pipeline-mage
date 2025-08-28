ATTACH TABLE _ UUID 'a4cf585b-59e9-40f4-8e1c-23416b982377'
(
    `shipping_date` String,
    `days_for_shipment_scheduled` Int32,
    `days_for_shipping_real` Int32,
    `shipping_mode` String,
    `delivery_status` String
)
ENGINE = MergeTree
ORDER BY shipping_date
SETTINGS index_granularity = 8192
