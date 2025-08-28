ATTACH TABLE _ UUID 'a42cfbd8-3238-428d-bb85-ecdef7d15761'
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
