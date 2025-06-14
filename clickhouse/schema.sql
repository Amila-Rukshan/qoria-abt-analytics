CREATE TABLE transactions (
    transaction_id String,
    transaction_date Date,
    user_id String,
    country LowCardinality(String),
    region LowCardinality(String),
    product_id String,
    product_name LowCardinality(String),
    category LowCardinality(String),
    price Decimal(10, 2),
    quantity UInt16,
    total_price Decimal(12, 2),
    stock_quantity UInt16,
    added_date Date
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(transaction_date)
ORDER BY (transaction_date, country, region, product_name)
