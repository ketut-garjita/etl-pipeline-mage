ATTACH TABLE _ UUID '72bc2ee7-897b-4637-a74d-d2dffba27160'
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
