import argparse
import json
import os
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Optional, Tuple

import boto3
import polars as pl
import psycopg2
from botocore.exceptions import ClientError
try:
    from dotenv import find_dotenv, load_dotenv
except ImportError: 
    def find_dotenv(*_args, **_kwargs) -> str:
        return ""

    def load_dotenv(*_args, **_kwargs) -> bool:
        return False
from psycopg2 import sql


def load_env() -> None:
    """Load .env dari cwd, folder script, atau parent — robust untuk runner beda (Kestra / local)."""
    candidates = [
        find_dotenv(usecwd=True),
        find_dotenv(),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"),
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"),
    ]
    for path in candidates:
        if path and os.path.exists(path):
            if load_dotenv(path):
                print(f"Loaded env dari {path}")
                return

UPDATED_AT_COLUMN = "updated_at"


@dataclass
class TableSpec:
    name: str
    unique_column: Optional[str] = None
    query: Optional[str] = None


def require_env(*keys: str, default: Optional[str] = None) -> str:
    for key in keys:
        value = os.getenv(key)
        if value:
            return value
    if default is not None:
        return default
    joined = ", ".join(keys)
    raise ValueError(f"Env {joined} wajib diisi (env var atau .env).")


def build_pg_config() -> dict:
    return {
        "host": require_env("POSTGRES_SOURCE_HOST", "POSTGRES_HOST", default="host.docker.internal"),
        "port": int(require_env("POSTGRES_SOURCE_PORT", "POSTGRES_PORT", default="5432")),
        "dbname": require_env(
            "POSTGRES_SOURCE_DB",
            "POSTGRES_SOURCE_DATABASE",
            "POSTGRES_DB",
            default="postgres",
        ),
        "user": require_env("POSTGRES_SOURCE_USER", "POSTGRES_USER"),
        "password": require_env("POSTGRES_SOURCE_PASSWORD", "POSTGRES_PASSWORD"),
    }


def build_s3_client() -> boto3.client:
    access_key = require_env("MINIO_ROOT_USER", "MINIO_ACCESS_KEY", "AWS_ACCESS_KEY_ID")
    secret_key = require_env("MINIO_ROOT_PASSWORD", "MINIO_SECRET_KEY", "AWS_SECRET_ACCESS_KEY")
    endpoint_url = (
        os.getenv("MINIO_ENDPOINT_URL")
        or os.getenv("S3_ENDPOINT_URL")
        or os.getenv("AWS_ENDPOINT_URL")
    )
    if not endpoint_url:
        host = os.getenv("MINIO_HOST")
        if host:
            port = os.getenv("MINIO_PORT") or os.getenv("MINIO_API_PORT") or "9000"
            endpoint_url = f"http://{host}:{port}"
        else:
            port = require_env("MINIO_API_PORT", default="9000")
            endpoint_url = f"http://host.docker.internal:{port}"
    return boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint_url,
        region_name="us-east-1",
    )


def parse_table_token(raw: str, default_unique: Optional[str]) -> TableSpec:
    parts = [part.strip() for part in raw.split(":")]
    table_name = parts[0] if parts else ""
    if not table_name:
        raise ValueError("Nama table tidak valid.")
    unique_column = parts[1] if len(parts) > 1 and parts[1] else default_unique
    return TableSpec(name=table_name, unique_column=unique_column)


def parse_table_config_data(data: object, default_unique: Optional[str]) -> list[TableSpec]:
    if not isinstance(data, list):
        raise ValueError("table-config harus berupa list JSON.")
    specs: list[TableSpec] = []
    for item in data:
        if isinstance(item, str):
            specs.append(parse_table_token(item, default_unique))
            continue
        if not isinstance(item, dict):
            raise ValueError("table-config harus berisi objek atau string.")
        table_name = item.get("table") or item.get("table_name")
        if not table_name:
            raise ValueError("Setiap table-config harus punya field 'table' atau 'table_name'.")
        unique_column = item.get("unique_column") or item.get("unique_col") or default_unique
        query = item.get("query")
        specs.append(
            TableSpec(
                name=table_name,
                unique_column=unique_column,
                query=query,
            )
        )
    return specs


def parse_table_specs(
    table_input: Optional[str],
    default_unique: Optional[str],
) -> list[TableSpec]:
    if not table_input:
        return []
    table_input = table_input.strip()
    if not table_input:
        return []
    if table_input.startswith("["):
        try:
            data = json.loads(table_input)
        except json.JSONDecodeError:
            data = None
        else:
            return parse_table_config_data(data, default_unique)
    specs: list[TableSpec] = []
    for raw in table_input.split(","):
        raw = raw.strip()
        if not raw:
            continue
        specs.append(parse_table_token(raw, default_unique))
    return specs


def load_table_config(config_path: str, default_unique: Optional[str]) -> list[TableSpec]:
    with open(config_path, "r", encoding="utf-8") as config_file:
        data = json.load(config_file)
    return parse_table_config_data(data, default_unique)


def load_table_config_json(config_json: str, default_unique: Optional[str]) -> list[TableSpec]:
    data = json.loads(config_json)
    return parse_table_config_data(data, default_unique)


def normalize_prefix(prefix: Optional[str]) -> str:
    if not prefix:
        return ""
    return prefix.strip().strip("/")


def table_identifier(table_name: str) -> sql.Identifier:
    parts = [part.strip() for part in table_name.split(".") if part.strip()]
    if not parts:
        raise ValueError("Nama table tidak valid.")
    return sql.Identifier(*parts)


def build_query(table: TableSpec, last_value: Optional[object], last_unique: Optional[object]) -> Tuple[sql.SQL, tuple]:
    if table.query:
        base_query = sql.SQL(table.query.strip().rstrip(";"))
    else:
        base_query = sql.SQL("SELECT * FROM {}").format(table_identifier(table.name))

    if last_value is None:
        return base_query, ()

    col_ref = sql.SQL("src.{}").format(sql.Identifier(UPDATED_AT_COLUMN))
    if table.unique_column and last_unique is not None:
        uniq_ref = sql.SQL("src.{}").format(sql.Identifier(table.unique_column))
        query = sql.SQL(
            "SELECT * FROM ({base}) AS src "
            "WHERE ({col} > %s OR ({col} = %s AND {uniq} > %s)) "
            "ORDER BY {col}, {uniq}"
        ).format(base=base_query, col=col_ref, uniq=uniq_ref)
        return query, (last_value, last_value, last_unique)

    query = sql.SQL(
        "SELECT * FROM ({base}) AS src WHERE {col} > %s ORDER BY {col}"
    ).format(base=base_query, col=col_ref)
    return query, (last_value,)


def read_state(s3_client, bucket: str, state_key: str) -> tuple[Optional[object], Optional[object]]:
    try:
        response = s3_client.get_object(Bucket=bucket, Key=state_key)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code in {"NoSuchKey", "404", "NotFound"}:
            return None, None
        raise
    payload = json.loads(response["Body"].read().decode("utf-8"))
    return payload.get("last_value"), payload.get("last_unique")


def write_state(
    s3_client,
    bucket: str,
    state_key: str,
    table: TableSpec,
    last_value: Optional[object],
    last_unique: Optional[object],
) -> None:
    if last_value is None:
        return
    serialized_last_value = last_value.isoformat() if isinstance(last_value, datetime) else last_value
    payload = {
        "table": table.name,
        "incremental_column": UPDATED_AT_COLUMN,
        "unique_column": table.unique_column,
        "last_value": serialized_last_value,
        "last_unique": last_unique,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    body = json.dumps(payload, default=str).encode("utf-8")
    s3_client.put_object(Bucket=bucket, Key=state_key, Body=body)


def update_last_values(
    rows: Iterable[tuple],
    inc_idx: Optional[int],
    uniq_idx: Optional[int],
    last_value: Optional[object],
    last_unique: Optional[object],
) -> tuple[Optional[object], Optional[object]]:
    if inc_idx is None:
        return last_value, last_unique
    for row in rows:
        inc_value = row[inc_idx]
        if inc_value is None:
            continue
        if last_value is None or inc_value > last_value:
            last_value = inc_value
            last_unique = row[uniq_idx] if uniq_idx is not None else last_unique
            continue
        if uniq_idx is not None and inc_value == last_value:
            uniq_value = row[uniq_idx]
            if uniq_value is None:
                continue
            if last_unique is None or uniq_value > last_unique:
                last_unique = uniq_value
    return last_value, last_unique


def write_parquet_chunks(
    parquet_path: str, rows_iter: Iterable[list[tuple]], columns: list[str]
) -> None:
    if hasattr(pl, "ParquetWriter"):
        writer = pl.ParquetWriter(parquet_path)
        for rows in rows_iter:
            if not rows:
                continue
            frame = pl.DataFrame(rows, schema=columns, orient="row")
            writer.write(frame)
        writer.close()
        return

    frames: list[pl.DataFrame] = []
    for rows in rows_iter:
        if not rows:
            continue
        frames.append(pl.DataFrame(rows, schema=columns, orient="row"))
    if not frames:
        return
    pl.concat(frames).write_parquet(parquet_path)


def parse_state_datetime(value: Optional[object]) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError as exc:
            raise ValueError("Format last_value pada state tidak valid.") from exc
    raise ValueError("Tipe last_value pada state tidak valid.")


def process_table(
    conn,
    s3_client,
    bucket: str,
    prefix: str,
    table: TableSpec,
    chunk_size: int,
) -> None:
    safe_table = table.name.replace(".", "_")
    prefix = normalize_prefix(prefix)
    parquet_prefix = f"{prefix}/parquet/{safe_table}" if prefix else f"parquet/{safe_table}"
    state_key = f"{prefix}/state/{safe_table}.json" if prefix else f"state/{safe_table}.json"

    last_value, last_unique = read_state(s3_client, bucket, state_key)
    last_value = parse_state_datetime(last_value)

    query, params = build_query(table, last_value, last_unique)

    with conn.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchmany(chunk_size)
        if not rows:
            print(f"[{table.name}] Tidak ada data baru, skip.")
            sys.stdout.flush()
            return

        colnames = [desc[0] for desc in cursor.description]
        if UPDATED_AT_COLUMN not in colnames:
            raise ValueError(f"Kolom {UPDATED_AT_COLUMN} tidak ditemukan di {table.name}.")
        inc_idx = colnames.index(UPDATED_AT_COLUMN)
        uniq_idx = None
        if table.unique_column:
            if table.unique_column not in colnames:
                raise ValueError(f"Kolom unique {table.unique_column} tidak ditemukan di {table.name}.")
            uniq_idx = colnames.index(table.unique_column)

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        parquet_name = f"{safe_table}_{timestamp}.parquet"
        parquet_key = f"{parquet_prefix}/{parquet_name}"

        with tempfile.TemporaryDirectory() as tmp_dir:
            parquet_path = os.path.join(tmp_dir, parquet_name)

            def row_batches() -> Iterable[list[tuple]]:
                nonlocal rows, last_value, last_unique
                while rows:
                    last_value, last_unique = update_last_values(
                        rows, inc_idx, uniq_idx, last_value, last_unique
                    )
                    yield rows
                    rows = cursor.fetchmany(chunk_size)

            write_parquet_chunks(parquet_path, row_batches(), colnames)
            s3_client.upload_file(parquet_path, bucket, parquet_key)

        print(f"[{table.name}] Upload selesai → {parquet_key}")
        sys.stdout.flush()

    write_state(s3_client, bucket, state_key, table, last_value, last_unique)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract Postgres incremental (updated_at) → Parquet ke MinIO.")
    parser.add_argument(
        "tables_pos",
        nargs="?",
        help="[OPSIONAL] Positional tables (untuk Kestra). Sama seperti --tables.",
    )
    parser.add_argument(
        "bucket_pos",
        nargs="?",
        help="[OPSIONAL] Positional bucket (untuk Kestra). Sama seperti --bucket.",
    )
    parser.add_argument(
        "prefix_pos",
        nargs="?",
        help="[OPSIONAL] Positional prefix (untuk Kestra). Sama seperti --prefix.",
    )
    parser.add_argument(
        "--tables",
        help=(
            "[WAJIB] Daftar table: schema.table[:unique_col] (pisah koma). "
            "Bisa diganti --table-config/--table-config-json/TABLES."
        ),
    )
    parser.add_argument(
        "--table-config",
        help="[OPSIONAL] Path JSON berisi list table config (alternatif --tables).",
    )
    parser.add_argument(
        "--table-config-json",
        default=os.getenv("TABLE_CONFIG_JSON"),
        help="[OPSIONAL] Isi JSON list table config (alternatif --tables / file).",
    )
    parser.add_argument(
        "--bucket",
        default=os.getenv("MINIO_BUCKET"),
        help="[WAJIB] Nama bucket MinIO.",
    )
    parser.add_argument(
        "--prefix",
        default=os.getenv("MINIO_PREFIX", ""),
        help="[OPSIONAL] Prefix object di MinIO (default: kosong).",
    )
    parser.add_argument(
        "--unique-column",
        default=os.getenv("UNIQUE_COLUMN"),
        help="Default kolom unique untuk tie-breaker.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=int(os.getenv("CHUNK_SIZE", "100000")),
        help="Jumlah baris per batch.",
    )
    args = parser.parse_args()
    if not args.tables and args.tables_pos:
        args.tables = args.tables_pos
    if not args.bucket and args.bucket_pos:
        args.bucket = args.bucket_pos
    if not args.prefix and args.prefix_pos:
        args.prefix = args.prefix_pos
    if not args.tables and not args.table_config and not args.table_config_json and not os.getenv("TABLES"):
        raise ValueError(
            "Wajib isi --tables/--table-config/--table-config-json, atau set env TABLES/TABLE_CONFIG_JSON."
        )
    if not args.bucket:
        raise ValueError("Bucket MinIO wajib diisi (--bucket atau MINIO_BUCKET di .env).")
    return args


def main() -> None:
    load_env()
    args = parse_args()

    table_specs: list[TableSpec] = []
    if args.table_config:
        table_specs.extend(load_table_config(args.table_config, args.unique_column))
    if args.table_config_json:
        table_specs.extend(load_table_config_json(args.table_config_json, args.unique_column))

    tables_input = args.tables or os.getenv("TABLES")
    table_specs.extend(parse_table_specs(tables_input, args.unique_column))

    if not table_specs:
        raise ValueError("Tidak ada table yang diproses.")

    pg_config = build_pg_config()
    s3_client = build_s3_client()

    with psycopg2.connect(**pg_config) as conn:
        for table in table_specs:
            process_table(conn, s3_client, args.bucket, args.prefix, table, args.chunk_size)


if __name__ == "__main__":
    main()
