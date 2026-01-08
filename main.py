#!/usr/bin/env python3
import os
import json
import subprocess
import time
from datetime import date, timedelta, datetime, timezone
from typing import Any, Dict, List

import requests
from google.cloud import bigquery

# ----------------------------
# CONFIG (env vars)
# ----------------------------
AUTH_URL = "https://vmos2.vmos.io/user/v1/auth"
CUBE_URL = "https://reporting.data.vmos.io/cubejs-api/v1/load"

PAGE_SIZE = 5000
MAX_PAGES = 50
MAX_POLLS = 25
REQUEST_TIMEOUT = 60  # seconds


def require_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing required env var: {name}")
    return val


def load_env():
    """
    Reads settings from environment variables.
    Local test: export these in your terminal first.
    Cloud Run: set these in the Console when deploying.
    """
    cfg = {
        "VM_USERNAME": require_env("VM_USERNAME"),
        "VM_PASSWORD": require_env("VM_PASSWORD"),
        "BQ_PROJECT": require_env("BQ_PROJECT"),
        "BQ_DATASET": require_env("BQ_DATASET"),
        "BQ_TABLE_FINAL": require_env("BQ_TABLE_FINAL"),
        "LOOKBACK_DAYS": int(os.getenv("LOOKBACK_DAYS", "3")),
    }
    return cfg


# ----------------------------
# VM Auth (curl)
# ----------------------------
def get_access_token_via_curl(username: str, password: str) -> str:
    curl_cmd = [
        "curl", "--silent", "--location", AUTH_URL,
        "--header", "Content-Type: application/json",
        "--header", "x-requested-from: management",
        "--data-raw", json.dumps({"email": username, "password": password}),
    ]
    result = subprocess.run(curl_cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"curl auth failed: {result.stderr}")

    resp_json = json.loads(result.stdout)
    return resp_json["payload"]["token"]["value"]


def make_headers(token: str) -> Dict[str, str]:
    return {
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Authorization": token,
    }


# ----------------------------
# Cube query runner
# ----------------------------
def run_cube_query(token: str, query: Dict[str, Any], vm_username: str, vm_password: str) -> Dict[str, Any]:
    headers = make_headers(token)

    for poll in range(1, MAX_POLLS + 1):
        try:
            resp = requests.post(
                CUBE_URL,
                headers=headers,
                json={"query": query},
                timeout=REQUEST_TIMEOUT,
            )
        except requests.exceptions.ReadTimeout:
            print(f"⏰ Read timeout (poll {poll}/{MAX_POLLS}) – retrying…")
            continue

        # Token expiry handling
        if resp.status_code == 500 and ("jwt expired" in resp.text.lower() or "TokenExpiredError" in resp.text):
            print("🔑 JWT expired – refreshing token…")
            token = get_access_token_via_curl(vm_username, vm_password)
            headers = make_headers(token)
            continue

        if resp.status_code >= 400:
            raise RuntimeError(f"HTTP {resp.status_code}: {resp.text}")

        data = resp.json()

        if data.get("error") == "Continue wait":
            print(f"… Continue wait (poll {poll}/{MAX_POLLS}) – sleeping 5s")
            time.sleep(5)
            continue

        return data

    raise RuntimeError("Continue wait exceeded max polls")


def build_query(start: date, end: date, limit: int, offset: int) -> Dict[str, Any]:
    return {
        "dimensions": [
            "OrderItems.orderUuid",
            "OrderItems.store",
            "OrderItems.channel",
            "OrderItems.createdAt",
        ],
        "measures": [
            "OrderItems.netSales",
            "OrderItems.quantity",
        ],
        "timeDimensions": [
            {
                "dimension": "OrderItems.createdAt",
                "dateRange": [start.isoformat(), end.isoformat()],
            }
        ],
        "filters": [
            {"member": "OrderItems.isValid", "operator": "equals", "values": ["true"]}
        ],
        "limit": limit,
        "offset": offset,
    }


def fetch_all_rows_for_range(token: str, start: date, end: date, vm_username: str, vm_password: str) -> List[Dict[str, Any]]:
    all_rows: List[Dict[str, Any]] = []

    for page in range(MAX_PAGES):
        offset = page * PAGE_SIZE
        print(f"📄 Page {page + 1} (offset {offset})")

        query = build_query(start, end, PAGE_SIZE, offset)
        data = run_cube_query(token, query, vm_username, vm_password)

        rows = data.get("data") or data.get("results", [{}])[0].get("data", [])
        print(f"   📊 Retrieved {len(rows)} rows")

        if not rows:
            break

        all_rows.extend(rows)

        if len(rows) < PAGE_SIZE:
            break

    return all_rows


# ----------------------------
# Transform -> BigQuery rows
# ----------------------------
def transform(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    load_ts = datetime.now(timezone.utc).isoformat()

    for r in rows:
        created_at = r.get("OrderItems.createdAt")
        order_date = None
        order_created_at = None

        if created_at:
            created_clean = created_at.replace("Z", "")
            dt = datetime.fromisoformat(created_clean)
            order_date = dt.date().isoformat()
            order_created_at = dt.replace(tzinfo=timezone.utc).isoformat()

        out.append({
            "order_uuid": r.get("OrderItems.orderUuid"),
            "order_date": order_date,
            "store_name": r.get("OrderItems.store"),
            "channel": r.get("OrderItems.channel"),
            "net_sales": float(r["OrderItems.netSales"]) if r.get("OrderItems.netSales") is not None else None,
            "items_count": float(r["OrderItems.quantity"]) if r.get("OrderItems.quantity") is not None else None,
            "order_created_at": order_created_at,
            "load_timestamp": load_ts,
        })

    return out


# ----------------------------
# BigQuery
# ----------------------------
def bq_client(project: str) -> bigquery.Client:
    return bigquery.Client(project=project)


def ensure_table_exists(client: bigquery.Client, table_ref: str) -> None:
    try:
        client.get_table(table_ref)
        print(f"👍 Table exists: {table_ref}")
        return
    except Exception:
        print(f"📦 Creating table: {table_ref}")

    schema = [
        bigquery.SchemaField("order_uuid", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("order_date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("store_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("channel", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("net_sales", "NUMERIC", mode="NULLABLE"),
        bigquery.SchemaField("items_count", "NUMERIC", mode="NULLABLE"),
        bigquery.SchemaField("order_created_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("load_timestamp", "TIMESTAMP", mode="REQUIRED"),
    ]
    table = bigquery.Table(table_ref, schema=schema)
    client.create_table(table)
    print("✅ Created table")


def append_rows(client: bigquery.Client, table_ref: str, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        print("⚠️ No rows to load.")
        return 0

    job = client.load_table_from_json(
        rows,
        table_ref,
        job_config=bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND),
    )
    job.result()
    print(f"✅ Appended {len(rows)} rows into {table_ref}")
    return len(rows)


# ----------------------------
# MAIN
# ----------------------------
if __name__ == "__main__":
    cfg = load_env()

    # Pull last N days ending yesterday (avoid partial today)
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=cfg["LOOKBACK_DAYS"] - 1)

    print(f"📆 Fetching range: {start.isoformat()} → {end.isoformat()} (LOOKBACK_DAYS={cfg['LOOKBACK_DAYS']})")

    token = get_access_token_via_curl(cfg["VM_USERNAME"], cfg["VM_PASSWORD"])
    raw = fetch_all_rows_for_range(token, start, end, cfg["VM_USERNAME"], cfg["VM_PASSWORD"])
    print(f"✅ Total raw rows: {len(raw)}")

    cleaned = transform(raw)
    print(f"✅ Total cleaned rows: {len(cleaned)}")

    if not cleaned:
        print("⚠️ No rows returned; exiting.")
        raise SystemExit(0)

    client = bq_client(cfg["BQ_PROJECT"])
    table_ref = f"{cfg['BQ_PROJECT']}.{cfg['BQ_DATASET']}.{cfg['BQ_TABLE_FINAL']}"

    ensure_table_exists(client, table_ref)
    appended = append_rows(client, table_ref, cleaned)

    print(f"🎯 Done. Appended {appended} rows to {table_ref}.")