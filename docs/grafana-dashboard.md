# Dashboard Grafana — Manufacturing Analytics

Dashboard portofolio sederhana: 4 panel, masing-masing dari satu materialized view (datamart) di ClickHouse. Query sudah di-`*Merge()` sehingga langsung menampilkan nilai final. Dokumen ini berisi langkah setup lengkap: koneksi datasource, setting dashboard global, lalu konfigurasi per panel (query + tipe + mapping + axis/unit) agar dashboard tampil rapi.

## Prasyarat

1. Tabel utama dibuat: `infrastructure/clickhouse/schema.sql`
2. Materialized views dijalankan: `infrastructure/clickhouse/materialized_views.sql`
3. Container `clickhouse_dwh` dan `grafana_bi` berjalan (dari `docker-compose.yml`)

## Langkah 1 — Sambungkan ClickHouse ke Grafana

1. Buka Grafana: `http://localhost:3000`
2. **Connections → Data sources → Add data source** → pilih **ClickHouse**
3. Isi koneksi:
   - **Host**: `clickhouse:8123` (dari dalam container Grafana), atau `localhost:8126` dari host
   - **User**: `ch_user` / **Password**: `ch_password` / **Database**: `datawarehouse`
4. **Save & Test** (harus sukses).

## Langkah 2 — Setting Dashboard (global, biar rapih)

**Dashboards → New → New dashboard.**

1. **Dashboard settings** (ikon roda gigi di kanan atas):
   - **General**: isi *Title* `Manufacturing Analytics`.
   - **Variables → Add variable** `machine`:
     - Type: **Query**
     - Query: `SELECT DISTINCT machine_id FROM datawarehouse.mv_machine_daily`
     - Ini dipakai filter panel 2 & 3 (lihat bawah).
   - **Templating**: tidak wajib, tapi variable `machine` memudahkan filter.
2. **Time range** (kanan atas dashboard): pilih rentang data historis, misal **Custom range** `2026-04-26 00:00:00` s.d. `2026-05-26 23:59:59`. Tanpa ini, data historis (Apr–Mei) tidak tampil karena default Grafana hanya hari ini.

> Setelah Kestra load data baru, cukup ganti time range ke periode terbaru — query otomatis mengikuti.

3. **Layout**: buat 4 panel, atur posisi & ukuran pakai **drag** (grab baris bawah/tepi panel). Tata letak yang rapi:

```
┌──────────────────────────────────────────────┐
│ Panel 1: Produksi Harian   (lebar penuh)     │
├───────────────────────┬──────────────────────┤
│ Panel 2: Mesin        │ Panel 4: Anomaly    │
├───────────────────────┴──────────────────────┤
│ Panel 3: Sensor & Anomaly (lebar penuh)      │
└──────────────────────────────────────────────┘
```

## Langkah 3 — Buat 4 Panel

Cara umum untuk tiap panel: **Add visualization** → pilih datasource **ClickHouse** → mode **SQL** → tempel query → terapkan **Settings** di bawah → **Apply**.

### Panel 1: Produksi Harian

**Query:**
```sql
SELECT order_date,
       countMerge(order_count) AS orders,
       sumMerge(target_total)  AS target_qty,
       sumMerge(actual_total)  AS actual_qty,
       sumMerge(gap_total)     AS gap
FROM datawarehouse.mv_production_daily
GROUP BY order_date
ORDER BY order_date
```

**Settings:**
- Visualization: **Bar chart** (tab *Visualizations* → Bar chart)
- Title panel: `Produksi Harian`
- **Field → All fields**:
  - Unit: `none` (jumlah order) / `short` jika mau
  - *Standard options → Unit*: `short`
- **Axis** (X): `order_date` → format **Time** (otomatis; pastikan X-axis pakai field waktu)
- **Legend**: tampilkan (default) supaya seri `orders` / `actual_qty` / `target_qty` jelas
- *Optional*: matikan seri `gap` agar panel bersih (toggle warna di legend), atau biarkan.

---

### Panel 2: Pemanfaatan Mesin

**Query:**
```sql
SELECT machine_id,
       countMerge(order_count) AS orders,
       sumMerge(target_total)  AS target_qty,
       sumMerge(actual_total)  AS actual_qty,
       round(sumMerge(actual_total) / NULLIF(sumMerge(target_total), 0) * 100, 1) AS utilization_pct
FROM datawarehouse.mv_machine_daily
WHERE machine_id = '$machine'
GROUP BY machine_id
ORDER BY actual_qty DESC
```

**Settings:**
- Visualization: **Bar chart**
- Title panel: `Pemanfaatan Mesin`
- **Field → `utilization_pct`**:
  - *Standard options → Unit*: `percent (0-100)` (nilai sudah 0–100 dari query)
- **Field → `actual_qty`**: Unit `short`
- **Axis**: X = `machine_id` (kategorikal), Y = nilai
- **Legend**: sembunyikan field selain yang dibutuhkan; atau pertahankan `actual_qty` + `utilization_pct` saja.
- Kalau variable `machine` dibiarkan `All`, query menampilkan semua mesin; pilih mesin spesifik untuk fokus satu.

---

## Tips Merapikan Dashboard

- **Panel spacing**: seret panel agar tersusun grid; Grafana auto-snap.
- **Ganti warna**: klik nama seri di legend → palet warna di *Field → Color scheme* (default `Classic palette` atau `From thresholds`).
- **Unit yang konsisten** membuat angka mudah dibandingkan (semua qty pakai `short`, persentase pakai `percent (0-100)`).
- **Variable** `machine` membuat dashboard interaktif (pilih mesin → semua panel ikut filter).
- Untuk portofolio: ambil screenshot dashboard dengan time range yang menampilkan data.

