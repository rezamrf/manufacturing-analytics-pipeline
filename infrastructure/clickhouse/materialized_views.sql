-- =============================================================
-- Data mart: Materialized Views dari tabel utama (fact_*)
-- =============================================================
-- Baca langsung dari tabel utama, tanpa tabel target terpisah.
-- MV sendiri datamart (AggregatingMergeTree + AggregateFunction state).
-- POPULATE: data historis ter-agregasi otomatis saat dibuat.
-- Grafana membaca via sumMerge() / avgMerge() / countMerge().
--
-- Jalankan:
--   docker exec -i clickhouse_dwh clickhouse-client < materialized_views.sql
-- Re-run: DROP VIEW <nama_mv> dulu, lalu jalankan lagi.
-- =============================================================

-- 1. Produksi harian (tren order, target vs actual, gap)
CREATE MATERIALIZED VIEW datawarehouse.mv_production_daily
ENGINE = AggregatingMergeTree()
ORDER BY (order_date)
POPULATE
AS
SELECT
    assumeNotNull(toDate(start_time))                   AS order_date,
    countState()                                        AS order_count,
    sumState(target_qty)                                AS target_total,
    sumState(actual_qty)                                AS actual_total,
    sumState(toInt64(target_qty) - toInt64(actual_qty)) AS gap_total
FROM datawarehouse.fact_production_orders
WHERE start_time IS NOT NULL
GROUP BY order_date;

-- 2. Produksi per mesin per hari (pemanfaatan mesin, basis OEE)
CREATE MATERIALIZED VIEW datawarehouse.mv_machine_daily
ENGINE = AggregatingMergeTree()
ORDER BY (machine_id, order_date)
POPULATE
AS
SELECT
    assumeNotNull(machine_id)         AS machine_id,
    assumeNotNull(toDate(start_time)) AS order_date,
    countState()                      AS order_count,
    sumState(target_qty)              AS target_total,
    sumState(actual_qty)              AS actual_total
FROM datawarehouse.fact_production_orders
WHERE machine_id IS NOT NULL AND start_time IS NOT NULL
GROUP BY machine_id, order_date;

-- 3. Statistik sensor per jam (avg/max/min + anomaly)
CREATE MATERIALIZED VIEW datawarehouse.mv_sensor_hourly
ENGINE = AggregatingMergeTree()
ORDER BY (sensor_id, reading_hour)
POPULATE
AS
SELECT
    assumeNotNull(sensor_id)                 AS sensor_id,
    toStartOfHour(reading_timestamp)         AS reading_hour,
    avgState(reading_value)                  AS avg_val,
    maxState(reading_value)                  AS max_val,
    minState(reading_value)                  AS min_val,
    countState()                             AS readings,
    sumState(toUInt8(is_anomaly))            AS anomaly_count
FROM datawarehouse.fact_sensor_readings
WHERE sensor_id IS NOT NULL
GROUP BY sensor_id, reading_hour;

-- 4. Anomaly per hari (tren anomaly rate sensor)
CREATE MATERIALIZED VIEW datawarehouse.mv_anomaly_daily
ENGINE = AggregatingMergeTree()
ORDER BY (reading_date)
POPULATE
AS
SELECT
    toDate(reading_timestamp)                 AS reading_date,
    sumState(toUInt8(is_anomaly))             AS anomaly_count,
    countState()                              AS total_readings
FROM datawarehouse.fact_sensor_readings
GROUP BY reading_date;
