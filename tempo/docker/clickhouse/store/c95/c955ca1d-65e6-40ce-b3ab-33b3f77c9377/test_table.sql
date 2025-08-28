ATTACH TABLE _ UUID 'eae6583f-adb6-4bb1-be2f-17975c5fef3e'
(
    `id` UInt32,
    `name` String,
    `created_at` DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192
