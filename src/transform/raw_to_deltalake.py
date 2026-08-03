import argparse
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
import polars as pl
from botocore.exceptions import ClientError
from deltalake import write_deltalake

try:
    from dotenv import find_dotenv, load_dotenv
except ImportError:  
    def find_dotenv(*_args, **_kwargs) -> str:
        return ""

    def load_dotenv(*_args, **_kwargs) -> bool:
        return False


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

STATE_EXTENSION = ".parquet"


def require_env(*keys: str, default: str | None = None) -> str:
    for key in keys:
        value = os.getenv(key)
        if value:
            return value
    if default is not None:
        return default
    raise ValueError(f"Env {keys} wajib diisi (env var atau .env).")


def get_s3_client() -> boto3.client:
    access_key = require_env(
        "MINIO_ROOT_USER", "MINIO_ACCESS_KEY", "AWS_ACCESS_KEY_ID"
    )
    secret_key = require_env(
        "MINIO_ROOT_PASSWORD", "MINIO_SECRET_KEY", "AWS_SECRET_ACCESS_KEY"
    )
    endpoint = os.getenv("MINIO_ENDPOINT_URL")
    if not endpoint:
        host = require_env("MINIO_HOST", default="host.docker.internal")
        port = require_env("MINIO_PORT", "MINIO_API_PORT", default="9000")
        endpoint = f"http://{host}:{port}"
    return boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint,
        region_name="us-east-1",
    )


def raw_table_name(table: str) -> str:
    return table.replace(".", "_")


def clean_table_name(table: str) -> str:
    return table.rsplit(".", 1)[-1]


def normalize_prefix(prefix: str | None) -> str:
    if not prefix:
        return ""
    return prefix.strip().strip("/")


def list_parquet_keys(s3, bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])
    keys.sort()
    return keys


def read_state(s3, bucket: str, state_key: str) -> dict:
    try:
        response = s3.get_object(Bucket=bucket, Key=state_key)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in {"NoSuchKey", "404", "NotFound"}:
            return {}
        raise
    return json.loads(response["Body"].read().decode("utf-8"))


def write_state(s3, bucket: str, state_key: str, payload: dict) -> None:
    body = json.dumps(payload, default=str).encode("utf-8")
    s3.put_object(Bucket=bucket, Key=state_key, Body=body)


def download_keys(s3, bucket: str, keys: list[str], dest: Path) -> list[Path]:
    local_files: list[Path] = []
    for key in keys:
        local = dest / Path(key).name
        s3.download_file(bucket, key, str(local))
        local_files.append(local)
    return local_files


def basic_transform(df: pl.DataFrame) -> pl.DataFrame:
    """Transformasi dasar yang berlaku untuk semua tabel."""

    # Cast kolom waktu ke Datetime
    for col in df.columns:
        if (
            "timestamp" in col.lower()
            or "created_at" in col.lower()
            or "updated_at" in col.lower()
        ):
            if df[col].dtype != pl.Datetime:
                df = df.with_columns(pl.col(col).cast(pl.Datetime))

    # Dedup baris identik dalam batch baru
    df = df.unique()

    # Sort deterministik
    sort_cols = [
        col
        for col in df.columns
        if col == "id" or col.endswith("_id")
        or col in ("updated_at", "created_at", "timestamp")
    ]
    if sort_cols:
        df = df.sort(sort_cols)

    return df


def storage_options() -> dict:
    endpoint = (
        os.getenv("MINIO_ENDPOINT_URL")
        or f"http://{require_env('MINIO_HOST', default='host.docker.internal')}:"
        f"{require_env('MINIO_PORT', 'MINIO_API_PORT', default='9000')}"
    )
    return {
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        "AWS_ALLOW_HTTP": "true",
        "AWS_REGION": "us-east-1",
        "AWS_ACCESS_KEY_ID": require_env(
            "MINIO_ROOT_USER", "MINIO_ACCESS_KEY", "AWS_ACCESS_KEY_ID"
        ),
        "AWS_SECRET_ACCESS_KEY": require_env(
            "MINIO_ROOT_PASSWORD", "MINIO_SECRET_KEY", "AWS_SECRET_ACCESS_KEY"
        ),
        "AWS_ENDPOINT": endpoint,
    }


def process_table(s3, bucket: str, raw_prefix: str, delta_prefix: str, table: str) -> None:
    raw_name = raw_table_name(table)
    clean_name = clean_table_name(table)

    raw_prefix = normalize_prefix(raw_prefix)
    delta_prefix = normalize_prefix(delta_prefix)

    raw_folder = f"{raw_prefix}/{raw_name}"
    delta_folder = f"{delta_prefix}/{clean_name}"
    state_key = f"{delta_prefix}/state/{clean_name}.json"

    print(f"[{table}] Raw: {raw_folder}, Delta: {delta_folder}")

    state = read_state(s3, bucket, state_key)
    last_file = state.get("last_processed_file")

    all_keys = list_parquet_keys(s3, bucket, raw_folder + "/")
    if not all_keys:
        print(f"[{table}] Tidak ada parquet, skip.")
        return

    # Filter hanya file yang belum diproses (nama file sortable lexicographic)
    new_keys = [k for k in all_keys if k > (f"{raw_folder}/{last_file}" if last_file else "")]
    if not new_keys:
        print(f"[{table}] Tidak ada parquet baru, skip.")
        return

    print(f"[{table}] Parquet baru: {len(new_keys)} file(s) -> {new_keys}")

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        local_files = download_keys(s3, bucket, new_keys, tmp_path)

        frames = [pl.read_parquet(f) for f in local_files]
        df = pl.concat(frames) if len(frames) > 1 else frames[0]

        print(f"[{table}] Raw rows: {len(df)}, columns: {df.columns}")

        df = basic_transform(df)

        print(f"[{table}] Clean rows: {len(df)}")

        if df.is_empty():
            print(f"[{table}] Hasil transform kosong, skip.")
            return

        delta_path = f"s3://{bucket}/{delta_folder}/"
        write_deltalake(
            delta_path,
            df.to_arrow(),
            mode="append",
            storage_options=storage_options(),
        )
        print(f"[{table}] Append selesai -> {delta_path}")

    # Update state: file terakhir (sorted) yang sudah diproses
    last_processed = Path(new_keys[-1]).name
    write_state(
        s3,
        bucket,
        state_key,
        {
            "table": clean_name,
            "last_processed_file": last_processed,
            "processed_files": len(new_keys),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    print(f"[{table}] State updated: {last_processed}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Tahap 3: Transformasi Parquet incremental -> Delta Lake di MinIO"
    )
    parser.add_argument(
        "--tables",
        required=True,
        help="Daftar tabel: schema.table (pisah koma). Contoh: public.dim_machines",
    )
    parser.add_argument(
        "--bucket",
        default=os.getenv("MINIO_BUCKET", "manufacturing"),
        help="Bucket MinIO (default: manufacturing)",
    )
    parser.add_argument(
        "--raw-prefix",
        default=os.getenv("MINIO_PREFIX", "raw-data/parquet"),
        help="Prefix parquet di MinIO (default: raw-data/parquet)",
    )
    parser.add_argument(
        "--delta-prefix",
        default="delta",
        help="Prefix Delta table di MinIO (default: delta)",
    )
    return parser.parse_args()


def main() -> None:
    load_env()
    args = parse_args()
    table_list = [t.strip() for t in args.tables.split(",") if t.strip()]
    if not table_list:
        raise ValueError("Minimal satu tabel harus diberikan.")

    s3 = get_s3_client()
    for table in table_list:
        process_table(s3, args.bucket, args.raw_prefix, args.delta_prefix, table)


if __name__ == "__main__":
    main()
