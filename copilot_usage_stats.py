# =============================================================================
# Copilot Usage Stats — Bronze Ingestion
# Fetches JSON reports from GitLab repo ait/copilot-usage-stats and writes
# them to a Delta table in Databricks.
# =============================================================================

import sys
import re
import time
import json
import urllib.parse
import requests
import pandas as pd
from datetime import datetime, timezone

from pyspark.sql import SparkSession, Row

spark = SparkSession.builder.getOrCreate()

# ── Config ───────────────────────────────────────────────────────────────────
sys.path.append("/Workspace/Users/mbrdej@inpost.pl/API_handlers")
from api_keys import gitlab_token_max

GITLAB_API_URL = "https://git.easypack24.net/api/v4"
CATALOG = "dev"
SCHEMA = "tmr_team"
TABLE_PREFIX = f"{CATALOG}.{SCHEMA}.bronze_copilot"
PARQUET_BASE_PATH = f"/mnt/{CATALOG}/{SCHEMA}/copilot_usage"
JSON_BASE_PATH = f"/mnt/{CATALOG}/{SCHEMA}/copilot_usage_json"
ERROR_LOG_TABLE = f"{CATALOG}.{SCHEMA}.copilot_usage_error_logs"

PROJECT_PATH = "ait/copilot-usage-stats"
PROJECT_PATH_ENCODED = urllib.parse.quote(PROJECT_PATH, safe="")
BRANCH = "master"
REPORTS_PATH = "reports"

TIMEOUT = 25
REQUEST_DELAY = 0.2

HEADERS = {"PRIVATE-TOKEN": gitlab_token_max}

script_name = "copilot_usage_stats"


# ── Error logging ────────────────────────────────────────────────────────────
_error_table_initialized = False


def log_error(msg, level="error"):
    global _error_table_initialized
    try:
        if not _error_table_initialized:
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {ERROR_LOG_TABLE} (
                    timestamp   TIMESTAMP,
                    script_name STRING,
                    level       STRING,
                    message     STRING
                ) USING delta
            """)
            _error_table_initialized = True

        row = Row(
            timestamp=datetime.now(timezone.utc),
            script_name=script_name,
            level=level,
            message=msg,
        )
        spark.createDataFrame([row]).write.format("delta").mode("append").saveAsTable(ERROR_LOG_TABLE)
    except Exception as e:
        print(f"⚠️ log_error failed: {e}")
        print(f"⚠️ original message: {msg}")


# ── GitLab API helpers ───────────────────────────────────────────────────────

def list_json_files():
    """List all .json files under reports/ in the repo."""
    per_page = 100
    page = 1
    json_files = []

    while True:
        url = f"{GITLAB_API_URL}/projects/{PROJECT_PATH_ENCODED}/repository/tree"
        params = {
            "path": REPORTS_PATH,
            "ref": BRANCH,
            "per_page": per_page,
            "page": page,
        }
        try:
            res = requests.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
        except requests.RequestException as e:
            msg = f"Request failed listing tree page {page}: {e}"
            print(f"❌ {msg}")
            log_error(msg)
            break

        if res.status_code != 200:
            msg = f"HTTP {res.status_code} listing tree page {page}: {res.text[:300]}"
            print(f"❌ {msg}")
            log_error(msg)
            break

        batch = res.json()
        if not batch:
            break

        for item in batch:
            if item.get("type") == "blob" and item.get("name", "").endswith(".json"):
                json_files.append({"name": item["name"], "path": item["path"]})

        if len(batch) < per_page:
            break
        page += 1
        time.sleep(REQUEST_DELAY)

    return json_files


def fetch_file_content(file_path):
    """Download and parse a single JSON file from the repo."""
    encoded_path = urllib.parse.quote(file_path, safe="")
    url = f"{GITLAB_API_URL}/projects/{PROJECT_PATH_ENCODED}/repository/files/{encoded_path}/raw"
    params = {"ref": BRANCH}

    try:
        res = requests.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
    except requests.RequestException as e:
        msg = f"Request failed for {file_path}: {e}"
        print(f"❌ {msg}")
        log_error(msg)
        return None

    if res.status_code != 200:
        msg = f"HTTP {res.status_code} for {file_path}: {res.text[:300]}"
        print(f"❌ {msg}")
        log_error(msg)
        return None

    try:
        return res.json()
    except json.JSONDecodeError as e:
        msg = f"JSON decode error for {file_path}: {e}"
        print(f"❌ {msg}")
        log_error(msg)
        return None


# ── Main ─────────────────────────────────────────────────────────────────────

def save_raw_json(file_name, content):
    """Save the original JSON content to DBFS as-is."""
    json_path = f"{JSON_BASE_PATH}/{file_name}"
    dbutils.fs.put(json_path, json.dumps(content, indent=2), overwrite=True)
    print(f"  ✅ JSON: {json_path}")


def file_name_to_table_suffix(file_name):
    """Turn 'Some-Report 2024.json' into 'some_report_2024'."""
    stem = file_name.rsplit(".", 1)[0]
    suffix = re.sub(r"[^a-z0-9]+", "_", stem.lower()).strip("_")
    return suffix


def run():
    print(f"Listing JSON files in {PROJECT_PATH}/{REPORTS_PATH}/ ...")
    json_files = list_json_files()
    print(f"Found {len(json_files)} JSON file(s).")

    if not json_files:
        print("Nothing to process. Exiting.")
        return

    created_delta = []
    created_parquet = []
    created_json = []
    errors = []

    for file_info in json_files:
        file_path = file_info["path"]
        file_name = file_info["name"]
        print(f"  Fetching {file_name} ...")

        content = fetch_file_content(file_path)
        time.sleep(REQUEST_DELAY)

        if content is None:
            errors.append(f"{file_name}: failed to fetch content")
            continue

        # Save original JSON file
        json_path = f"{JSON_BASE_PATH}/{file_name}"
        try:
            save_raw_json(file_name, content)
            created_json.append(json_path)
        except Exception as e:
            msg = f"{file_name}: JSON save failed — {e}"
            errors.append(msg)
            log_error(msg)

        # Build a Spark DataFrame from the raw JSON — no transformation
        records = content if isinstance(content, list) else [content]
        df = spark.createDataFrame(pd.json_normalize(records))

        suffix = file_name_to_table_suffix(file_name)
        delta_table = f"{TABLE_PREFIX}_{suffix}"
        parquet_path = f"{PARQUET_BASE_PATH}/{suffix}"

        # Write as Delta table
        try:
            (
                df.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(delta_table)
            )
            row_count = spark.table(delta_table).count()
            print(f"  ✅ Delta: {row_count} rows → {delta_table}")
            created_delta.append(delta_table)
        except Exception as e:
            msg = f"{file_name}: Delta write failed — {e}"
            errors.append(msg)
            log_error(msg)

        # Write as Parquet
        try:
            (
                df.write
                .format("parquet")
                .mode("overwrite")
                .save(parquet_path)
            )
            print(f"  ✅ Parquet: {parquet_path}")
            created_parquet.append(parquet_path)
        except Exception as e:
            msg = f"{file_name}: Parquet write failed — {e}"
            errors.append(msg)
            log_error(msg)

    # ── Summary ─────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    print(f"\nDelta tables ({len(created_delta)}):")
    for t in created_delta:
        print(f"  - {t}")

    print(f"\nParquet files ({len(created_parquet)}):")
    for p in created_parquet:
        print(f"  - {p}")

    print(f"\nJSON files ({len(created_json)}):")
    for j in created_json:
        print(f"  - {j}")

    if errors:
        print(f"\n❌ Errors ({len(errors)}):")
        for e in errors:
            print(f"  - {e}")
    else:
        print("\nNo errors.")

    print()


# ── Entry point ──────────────────────────────────────────────────────────────
run()
