#!/usr/bin/env python3
import os
import json
import subprocess
import time
from datetime import date, timedelta, datetime, timezone
from typing import Dict, Any, List

import requests
from google.cloud import bigquery

# ============================
# CONFIG
# ============================
AUTH_URL = "https://vmos2.vmos.io/user/v1/auth"
CUBE_URL = "https://reporting.data.vmos.io/cubejs-api/v1/load"

PAGE_SIZE = 5000
MAX_PAGES = 50
MAX_POLLS = 25
REQUEST_TIMEOUT = 60


# ============================
# ENV
# ============================
def require_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing required env var: {name}")
    return val


def load_env():
    return {
        "VM_USERNAME": require_env("VM_USERNAME"),
        "VM_PASSWORD": require_env("VM_PASSWORD"),
        "BQ_PROJECT": require_env("BQ_PROJECT"),
        "BQ_DATASET": require_env("BQ_DATASET"),
        "BQ_TABLE_STAGE": require_env("BQ_TABLE_STAGE"),
        "BQ_TABLE_FINAL": require_env("BQ_TABLE_FINAL"),
        "LOOKBACK_DAYS": int(os.getenv("LOOKBACK_DAYS", "3")),
    }


# ============================
# VM AUTH
# ============================
def get_access_token(username: str, password: str) -> str:
    cmd = [
        "curl", "--silent", "--location", AUTH_URL,
        "--header", "Content-Type: application/json",
        "--header", "x-requested-from: management",
        "--data-raw", json.dumps({"email": username, "password": password}),
    ]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(r.stderr)

    return json.loads(r.stdout)["payload"]["token"]["value"]


def headers(token: str):
    return {
        "Content-Type": "application/json",
        "Authorization": token,
    }


# ============================
# CUBE
# ============================
def cube_load(token, query, user, pwd):
    for i in range(MAX_POLLS):
        r = requests.post(CUBE_URL, headers=headers(token), json={"query": query}, timeout=REQUEST_TIMEOUT)

        if r.status_code == 500 and "jwt" in r.text.lower():
            token = get_access_token(user, pwd)
            continue

        if r.status_code >= 400:
            raise RuntimeError(r.text)

        data = r.json()
        if data.get("error") == "Continue wait":
            time.sleep(5)
            continue

        return data

    raise RuntimeError("Cube timeout")


def build_query(start, end, limit, offset):
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


def fetch(token, start, end, user, pwd):
    out = []
    for page in range(MAX_PAGES):
        data = cube_load(token, build_query(start, end, PAGE_SIZE, page * PAGE_SIZE), user, pwd)
        rows = data.get("data") or data.get("results", [{}])[0].get("data", [])
        if not rows:
            break
        out.extend(rows)
        if len(rows) < PAGE_SIZE:
            break
    return out


# ============================
# TRANSFORM
# ============================
def transform(rows):
    ts = datetime.now(timezone.utc).isoformat()
    out = []

    for r in rows:
        dt = datetime.fromisoformat(r["OrderItems.createdAt"].replace("Z", ""))
        out.append({
            "order_uuid": r["OrderItems.orderUuid"],
            "order_date": dt.date().isoformat(),
            "store_name": r["OrderItems.store"],
            "channel": r["OrderItems.channel"],
            "net_sales": float(r["OrderItems.netSales"]),
            "items_count": float(r["OrderItems.quantity"]),
            "order_created_at": dt.replace(tzinfo=timezone.utc).isoformat(),
            "load_timestamp": ts,
        })

    return out


# ============================
# BIGQUERY
# ============================
def load_stage(client, table, rows):
    job = client.load_table_from_json(
        rows,
        table,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
    )
    job.result()
    print(f"Loaded {len(rows)} → {table}")


def replace_partition(client, final, stage, start_date):
    sql = f"""
    BEGIN TRANSACTION;

    DELETE FROM `{final}` WHERE order_date >= DATE("{start_date}");

    INSERT INTO `{final}`
    (order_uuid, order_date, store_name, channel, net_sales, items_count, order_created_at, load_timestamp)
    SELECT
      order_uuid,
      order_date,
      store_name,
      channel,
      CAST(net_sales AS NUMERIC),
      CAST(items_count AS NUMERIC),
      order_created_at,
      load_timestamp
    FROM `{stage}`
    WHERE order_date >= DATE("{start_date}");

    COMMIT TRANSACTION;
    """
    client.query(sql).result()
    print("Canonical updated")


# ============================
# MAIN
# ============================
if __name__ == "__main__":
    cfg = load_env()

    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=cfg["LOOKBACK_DAYS"] - 1)

    print(f"Fetching {start} → {end}")

    token = get_access_token(cfg["VM_USERNAME"], cfg["VM_PASSWORD"])
    raw = fetch(token, start, end, cfg["VM_USERNAME"], cfg["VM_PASSWORD"])
    rows = transform(raw)

    if not rows:
        print("No rows returned")
        exit()

    client = bigquery.Client(project=cfg["BQ_PROJECT"])

    stage = f"{cfg['BQ_PROJECT']}.{cfg['BQ_DATASET']}.{cfg['BQ_TABLE_STAGE']}"
    final = f"{cfg['BQ_PROJECT']}.{cfg['BQ_DATASET']}.{cfg['BQ_TABLE_FINAL']}"

    load_stage(client, stage, rows)
    replace_partition(client, final, stage, start)

    print("Done.")