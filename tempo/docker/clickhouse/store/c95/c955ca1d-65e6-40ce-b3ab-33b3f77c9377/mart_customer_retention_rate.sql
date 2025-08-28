ATTACH TABLE _ UUID '38f2d202-1d8f-416c-8d29-5bd7ffc873f5'
(
    `year` Nullable(Int64),
    `month_name` Nullable(String),
    `distinct_customers` Nullable(Int64),
    `rolling_customers` Nullable(Float64),
    `customer_retention_rate` Nullable(Float64)
)
ENGINE = Memory
