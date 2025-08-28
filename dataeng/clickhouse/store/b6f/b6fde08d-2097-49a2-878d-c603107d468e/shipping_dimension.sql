ATTACH TABLE _ UUID 'd749d991-a5be-4c3d-bfa8-845c8435dbbb'
(
    `shipping_date` String,
    `days_for_shipment_scheduled` String,
    `days_for_shipping_real` String,
    `shipping_mode` String,
    `delivery_status` String
)
ENGINE = MergeTree
ORDER BY shipping_date
SETTINGS index_granularity = 8192
