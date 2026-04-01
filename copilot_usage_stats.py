# =============================================================================
# Copilot Usage Stats — Bronze Ingestion
# Fetches JSON reports from GitLab repo ait/copilot-usage-stats and writes
# them to a Delta table in Databricks.
# =============================================================================

import sys
import time
import json
import urllib.parse
import requests
import pandas as pd
from datetime import datetime, timezone

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import current_timestamp, lit

spark = SparkSession.builder.getOrCreate()

# ── Config ───────────────────────────────────────────────────────────────────
sys.path.append("/Workspace/Users/mbrdej@inpost.pl/API_handlers")
from api_keys import gitlab_token_max

GITLAB_API_URL = "https://git.easypack24.net/api/v4"
CATALOG = "dev"
SCHEMA = "tmr_team"
TABLE = f"{CATALOG}.{SCHEMA}.bronze_copilot_usage_stats"
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

def run():
    print(f"Listing JSON files in {PROJECT_PATH}/{REPORTS_PATH}/ ...")
    json_files = list_json_files()
    print(f"Found {len(json_files)} JSON file(s).")

    if not json_files:
        print("Nothing to process. Exiting.")
        return

    all_rows = []

    for file_info in json_files:
        file_path = file_info["path"]
        file_name = file_info["name"]
        print(f"  Fetching {file_name} ...")

        content = fetch_file_content(file_path)
        time.sleep(REQUEST_DELAY)

        if content is None:
            continue

        if isinstance(content, list):
            for record in content:
                row = {"source_file": file_name, "raw_json": json.dumps(record)}
                if isinstance(record, dict):
                    for k, v in record.items():
                        row[k] = str(v) if not isinstance(v, (str, type(None))) else v
                all_rows.append(row)
        elif isinstance(content, dict):
            row = {"source_file": file_name, "raw_json": json.dumps(content)}
            for k, v in content.items():
                row[k] = str(v) if not isinstance(v, (str, type(None))) else v
            all_rows.append(row)
        else:
            msg = f"Unexpected JSON type in {file_name}: {type(content).__name__}"
            print(f"⚠️ {msg}")
            log_error(msg, level="warning")
            all_rows.append({"source_file": file_name, "raw_json": json.dumps(content)})

    if not all_rows:
        print("No rows produced. Exiting.")
        return

    print(f"Building DataFrame with {len(all_rows)} row(s) ...")
    df_pd = pd.DataFrame(all_rows)
    df_spark = spark.createDataFrame(df_pd)
    df_spark = df_spark.withColumn("load_timestamp", current_timestamp())
    df_spark = df_spark.withColumn("source_system", lit("gitlab_repo_file"))

    (
        df_spark.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TABLE)
    )

    row_count = spark.table(TABLE).count()
    print(f"✅ Written {row_count} records to {TABLE}")


# ── Entry point ──────────────────────────────────────────────────────────────
run()
