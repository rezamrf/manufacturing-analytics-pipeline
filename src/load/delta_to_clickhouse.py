import argparse
import json
import os
from datetime import datetime, timezone

import clickhouse_connect
import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import DeltaTable
from deltalake.exceptions import DeltaError
from botocore.exceptions import ClientError

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


DEFAULT_UNIQUE_KEY = "id"
VERSION_COLUMN = "_delta_version"
BATCH_SIZE = 100_000


def require_env(*keys: str, default: str | None = None) -> str:
    for key in keys:
        value = os.getenv(key)
        if value:
            return value
    if default is not None:
        return default
    raise ValueError(f"Env {keys} wajib diisi (env var atau .env).")


def get_s3_client():
    import boto3

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


def get_clickhouse_client():
    host = require_env("CLICKHOUSE_HOST")
    port = int(require_env("CLICKHOUSE_HTTP_PORT", default="8123"))
    user = require_env("CLICKHOUSE_USER")
    password = require_env("CLICKHOUSE_PASSWORD")
    database = require_env("CLICKHOUSE_DB", "CLICKHOUSE_DATABASE", default="datawarehouse")
    return clickhouse_connect.get_client(
        host=host,
        port=port,
        username=user,
        password=password,
        database=database,
    )


def read_state(s3, bucket: str, state_key: str) -> dict:
    try:
        response = s3.get_object(Bucket=bucket, Key=state_key)
    except Exception as exc:
        code = getattr(exc, "response", {}).get("Error", {}).get("Code")
        if code in {"NoSuchKey", "404", "NotFound"}:
            return {}
        raise
    return json.loads(response["Body"].read().decode("utf-8"))


def write_state(s3, bucket: str, state_key: str, payload: dict) -> None:
    body = json.dumps(payload, default=str).encode("utf-8")
    s3.put_object(Bucket=bucket, Key=state_key, Body=body)


def normalize_timestamps(arrow_table: pa.Table) -> pa.Table:
    """Konversi kolom timestamptz (tz-aware) ke naive UTC untuk DateTime64 CH."""
    schema = arrow_table.schema
    updates: list[pa.Field] = []
    arrays: list[pa.Array] = []
    for field in schema:
        field_type = field.type
        if pa.types.is_timestamp(field_type) and field_type.tz is not None:
            arr = arrow_table.column(field.name).cast(pa.timestamp("us", tz="UTC")).cast(
                pa.timestamp("us", tz=None)
            )
            arrays.append(arr)
            updates.append(field.with_type(pa.timestamp("us", tz=None)))
        else:
            arrays.append(arrow_table.column(field.name))
            updates.append(field)
    return pa.Table.from_arrays(arrays, schema=pa.schema(updates))


def table_from_delta(table: str) -> str:
    return table.rsplit(".", 1)[-1]


def normalize_prefix(prefix: str | None) -> str:
    if not prefix:
        return ""
    return prefix.strip().strip("/")


def read_delta_log(s3, bucket: str, log_key: str) -> list[dict]:
    try:
        body = s3.get_object(Bucket=bucket, Key=log_key)["Body"].read().decode("utf-8")
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in {"NoSuchKey", "404", "NotFound"}:
            return []
        raise
    return [json.loads(line) for line in body.strip().splitlines() if line.strip()]


def list_delta_versions(s3, bucket: str, log_prefix: str) -> list[int]:
    versions: list[int] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=log_prefix):
        for obj in page.get("Contents", []):
            name = obj["Key"].rsplit("/", 1)[-1]
            if name.endswith(".json") and name.split(".")[0].isdigit():
                versions.append(int(name.split(".")[0]))
    return sorted(versions)


def new_parquet_paths(s3, bucket: str, log_prefix: str, version: int) -> list[str]:
    """Path parquet yang DITAMBAH di version tertentu (dari aksi 'add' pada log)."""
    paths: list[str] = []
    log_key = f"{log_prefix}{version:020d}.json"
    for action in read_delta_log(s3, bucket, log_key):
        if "add" in action:
            paths.append(action["add"]["path"])
    return paths


def load_table(
    s3,
    client,
    bucket: str,
    delta_prefix: str,
    database: str,
    table: str,
    unique_key: str,
) -> bool:
    clean_name = table_from_delta(table)
    delta_prefix = normalize_prefix(delta_prefix)

    delta_path = f"s3://{bucket}/{delta_prefix}/{clean_name}/"
    log_prefix = f"{delta_prefix}/{clean_name}/_delta_log/"
    state_key = f"{delta_prefix}/state/load/{clean_name}.json"

    try:
        dt = DeltaTable(delta_path, storage_options=storage_options())
    except DeltaError as exc:
        print(f"[{clean_name}] Gagal buka Delta table: {exc}")
        return False

    version = dt.version()
    state = read_state(s3, bucket, state_key)
    last_version = state.get("last_version")

    if last_version is not None and version <= int(last_version):
        print(f"[{clean_name}] Delta v{version} sudah dimuat (state v{last_version}), skip.")
        return True

    # Cuma version baru (last_version+1 .. version) yang diproses, bukan full snapshot.
    all_versions = list_delta_versions(s3, bucket, log_prefix)
    start = int(last_version) + 1 if last_version is not None else 0
    new_versions = [v for v in all_versions if v >= start]

    full_name = f"{database}.{clean_name}"
    total_rows = 0

    for v in new_versions:
        parquet_paths = new_parquet_paths(s3, bucket, log_prefix, v)
        if not parquet_paths:
            print(f"[{clean_name}] v{v} tanpa add parquet, skip.")
            continue

        frames: list[pa.Table] = []
        for rel_path in parquet_paths:
            key = f"{delta_prefix}/{clean_name}/{rel_path}"
            with pa.BufferReader(s3.get_object(Bucket=bucket, Key=key)["Body"].read()) as buf:
                frames.append(pq.read_table(buf))
        arrow_table = pa.concat_tables(frames) if len(frames) > 1 else frames[0]

        arrow_table = normalize_timestamps(arrow_table)
        version_array = pa.array([v] * arrow_table.num_rows, type=pa.uint64())
        arrow_table = arrow_table.append_column(VERSION_COLUMN, version_array)

        batches = arrow_table.to_batches(max_chunksize=BATCH_SIZE)
        try:
            for idx, batch in enumerate(batches, start=1):
                client.insert_arrow(full_name, batch)
                print(f"[{clean_name}] v{v} batch {idx}/{len(batches)} ter-load ({batch.num_rows} baris).")
        except Exception as exc:
            print(f"[{clean_name}] Gagal insert ke ClickHouse: {exc}")
            return False

        total_rows += arrow_table.num_rows

    write_state(
        s3,
        bucket,
        state_key,
        {
            "table": clean_name,
            "unique_key": unique_key,
            "last_version": version,
            "rows": total_rows,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    print(f"[{clean_name}] Load selesai s.d. v{version}: {total_rows} baris baru.")
    return True


def parse_unique_keys(raw: str | None) -> dict:
    mapping: dict[str, str] = {}
    if not raw:
        return mapping
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        parts = [p.strip() for p in token.split(":")]
        table_name = table_from_delta(parts[0])
        mapping[table_name] = parts[1] if len(parts) > 1 and parts[1] else DEFAULT_UNIQUE_KEY
    return mapping


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Tahap 4: Load incremental Delta Lake -> ClickHouse"
    )
    parser.add_argument(
        "--tables",
        required=True,
        help="Daftar tabel Delta (pisah koma). Contoh: dim_machines,dim_sensors",
    )
    parser.add_argument(
        "--unique-keys",
        help=(
            "Kolom unique key per tabel, format table:column (pisah koma). "
            f"Default: {DEFAULT_UNIQUE_KEY}. Contoh: dim_machines:machine_id"
        ),
    )
    parser.add_argument(
        "--bucket",
        default=os.getenv("MINIO_BUCKET", "manufacturing"),
        help="Bucket MinIO (default: manufacturing)",
    )
    parser.add_argument(
        "--delta-prefix",
        default="delta",
        help="Prefix Delta table di MinIO (default: delta)",
    )
    parser.add_argument(
        "--database",
        default=os.getenv("CLICKHOUSE_DB", "datawarehouse"),
        help="Database ClickHouse tujuan (default: datawarehouse)",
    )
    return parser.parse_args()


def main() -> None:
    load_env()
    args = parse_args()

    table_list = [t.strip() for t in args.tables.split(",") if t.strip()]
    if not table_list:
        raise ValueError("Minimal satu tabel harus diberikan.")

    unique_keys = parse_unique_keys(args.unique_keys)

    s3 = get_s3_client()
    client = get_clickhouse_client()

    for table in table_list:
        clean_name = table_from_delta(table)
        unique_key = unique_keys.get(clean_name, DEFAULT_UNIQUE_KEY)
        load_table(
            s3,
            client,
            args.bucket,
            args.delta_prefix,
            args.database,
            clean_name,
            unique_key,
        )


if __name__ == "__main__":
    main()
