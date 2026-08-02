-- Skema ClickHouse untuk tahap 4 (load Delta Lake -> DWH)
-- ReplacingMergeTree(_delta_version): 1 baris terbaru per unique key.
-- _delta_version = version Delta saat baris dimuat -> dedup idempoten.
-- Jalankan sekali: docker exec -i clickhouse_dwh clickhouse-client < infrastructure/clickhouse/schema.sql

CREATE TABLE IF NOT EXISTS datawarehouse.dim_machines (
    machine_id     String,
    machine_name   String,
    machine_type   String,
    location       String,
    status         Nullable(String),
    created_at     Nullable(DateTime64(6)),
    updated_at     Nullable(DateTime64(6)),
    _delta_version UInt64
) ENGINE = ReplacingMergeTree(_delta_version)
  ORDER BY (machine_id);

CREATE TABLE IF NOT EXISTS datawarehouse.dim_sensors (
    sensor_id        String,
    machine_id       Nullable(String),
    sensor_type      String,
    measurement_unit String,
    created_at       Nullable(DateTime64(6)),
    updated_at       Nullable(DateTime64(6)),
    _delta_version   UInt64
) ENGINE = ReplacingMergeTree(_delta_version)
  ORDER BY (sensor_id);

CREATE TABLE IF NOT EXISTS datawarehouse.fact_production_orders (
    order_id       String,
    machine_id     Nullable(String),
    product_sku    String,
    target_qty     Int32,
    actual_qty     Nullable(Int32),
    status         Nullable(String),
    start_time     Nullable(DateTime64(6)),
    end_time       Nullable(DateTime64(6)),
    created_at     Nullable(DateTime64(6)),
    updated_at     Nullable(DateTime64(6)),
    _delta_version UInt64
) ENGINE = ReplacingMergeTree(_delta_version)
  ORDER BY (order_id);

CREATE TABLE IF NOT EXISTS datawarehouse.fact_sensor_readings (
    reading_id        Int64,
    sensor_id         Nullable(String),
    reading_value     Decimal(10,4),
    is_anomaly        Nullable(Bool),
    reading_timestamp DateTime64(6),
    created_at        Nullable(DateTime64(6)),
    updated_at        Nullable(DateTime64(6)),
    _delta_version    UInt64
) ENGINE = ReplacingMergeTree(_delta_version)
  ORDER BY (reading_id);
