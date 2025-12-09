import argparse
import os
import sys
import yaml
import logging
import time
import requests

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("validation")


# --------------------------------------------------------
# Load YAML configuration
# --------------------------------------------------------
def load_config(path):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    full_path = os.path.join(project_root, path)

    if not os.path.exists(full_path):
        raise FileNotFoundError(f"Config file not found: {full_path}")

    with open(full_path, "r") as f:
        return yaml.safe_load(f)


# --------------------------------------------------------
# Run SQL using Databricks REST API (universal method)
# --------------------------------------------------------
def run_sql(host, token, warehouse_id, catalog, schema, table):
    base_url = f"{host}/api/2.0/sql/statements"
    headers = {"Authorization": f"Bearer {token}"}

    query = f"SELECT COUNT(*) AS c FROM `{catalog}`.`{schema}`.`{table}`"

    payload = {"statement": query, "warehouse_id": warehouse_id, "catalog": catalog, "schema": schema}

    # Submit statement
    r = requests.post(base_url, json=payload, headers=headers)
    if r.status_code != 200:
        raise RuntimeError(f"SQL submit failed: {r.text}")

    statement_id = r.json()["statement_id"]
    status_url = f"{base_url}/{statement_id}"

    # Poll until finished
    while True:
        s = requests.get(status_url, headers=headers).json()
        state = s["status"]["state"]

        if state in ("PENDING", "RUNNING"):
            time.sleep(1)
            continue

        if state == "FAILED":
            raise RuntimeError(f"SQL execution failed: {s}")

        break  # state == SUCCEEDED

    # -----------------------------------------------------
    # Extract count across all Databricks SQL API formats
    # -----------------------------------------------------

    # Format A: {"result": {"data_array": [[count]]}}
    if "result" in s and "data_array" in s["result"]:
        return int(s["result"]["data_array"][0][0])

    # Format B: {"response": {"result": {"data_array": [[count]]}}}
    if "response" in s and "result" in s["response"]:
        return int(s["response"]["result"]["data_array"][0][0])

    # Format C (older API): {"results": {"data": [[count]]}}
    if "results" in s and "data" in s["results"]:
        return int(s["results"]["data"][0][0])

    raise RuntimeError(f"Unknown SQL result format: {s}")


# --------------------------------------------------------
# Validate table existence via UC REST API
# --------------------------------------------------------
def validate_table_exists(host, token, catalog, schema, table):
    url = f"{host}/api/2.1/unity-catalog/tables/{catalog}.{schema}.{table}"
    headers = {"Authorization": f"Bearer {token}"}

    r = requests.get(url, headers=headers)
    return r.status_code == 200


# --------------------------------------------------------
# Validate all tables
# --------------------------------------------------------
def validate_tables(host, token, warehouse_id, tables):
    ok = True
    catalog = "dab-mvp-dev"  # FIXED for DEV environment only

    for t in tables:
        schema, table = t["name"].split(".", 1)
        min_rows = t.get("min_rows", 0)

        fqdn = f"{catalog}.{schema}.{table}"
        logger.info(f"Checking table: {fqdn}")

        # 1. Existence
        if not validate_table_exists(host, token, catalog, schema, table):
            logger.error(f"❌ Table does NOT exist: {fqdn}")
            ok = False
            continue

        # 2. Row count
        try:
            count = run_sql(host, token, warehouse_id, catalog, schema, table)
            if count < min_rows:
                logger.error(f"❌ Row count too low for {fqdn}: {count} < {min_rows}")
                ok = False
        except Exception as e:
            logger.error(f"❌ SQL error reading {fqdn}: {e}")
            ok = False

    return ok


# --------------------------------------------------------
# Validate Job existence
# --------------------------------------------------------
def validate_jobs(host, token, jobs):
    ok = True

    job_list_url = f"{host}/api/2.1/jobs/list"
    headers = {"Authorization": f"Bearer {token}"}

    r = requests.get(job_list_url, headers=headers)
    all_jobs = r.json().get("jobs", [])

    for entry in jobs:
        expected = entry["name"]
        logger.info(f"Checking job: {expected}")

        found = any(job.get("settings", {}).get("name") == expected for job in all_jobs)

        if not found:
            logger.error(f"❌ Job not found: {expected}")
            ok = False

    return ok


# --------------------------------------------------------
# MAIN
# --------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="validation_config.yml")
    args = parser.parse_args()

    cfg = load_config(args.config)

    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    warehouse_id = os.getenv("WAREHOUSE_ID")

    if not host or not token or not warehouse_id:
        logger.error("❌ Missing env vars: DATABRICKS_HOST / DATABRICKS_TOKEN / WAREHOUSE_ID")
        sys.exit(1)

    tables_ok = validate_tables(host, token, warehouse_id, cfg.get("tables", []))
    jobs_ok = validate_jobs(host, token, cfg.get("jobs", []))

    if tables_ok and jobs_ok:
        logger.info("✅ All DEV validations passed!")
        sys.exit(0)
    else:
        logger.error("❌ Validation FAILED!")
        sys.exit(1)


if __name__ == "__main__":
    main()
