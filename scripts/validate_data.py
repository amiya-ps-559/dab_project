import argparse
import os
import sys
import yaml
import logging
import time
import requests

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("validation")


# ------------------------------------------
# Load config
# ------------------------------------------
def load_config(path):
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    full_path = os.path.join(base_dir, path)
    if not os.path.exists(full_path):
        raise FileNotFoundError(f"Config file does not exist: {full_path}")
    with open(full_path, "r") as f:
        return yaml.safe_load(f)


# ------------------------------------------
# SQL execution via REST API
# ------------------------------------------
def run_sql(host, token, warehouse_id, catalog, schema, table):
    url = f"{host}/api/2.0/sql/statements"
    headers = {"Authorization": f"Bearer {token}"}

    query = f"SELECT COUNT(*) AS c FROM `{catalog}`.`{schema}`.`{table}`"

    payload = {"statement": query, "warehouse_id": warehouse_id, "catalog": catalog, "schema": schema}

    # Submit SQL
    r = requests.post(url, json=payload, headers=headers)
    if r.status_code != 200:
        raise RuntimeError(f"SQL submit failed: {r.text}")

    statement_id = r.json()["statement_id"]

    # Poll until complete
    status_url = f"{url}/{statement_id}"
    while True:
        s = requests.get(status_url, headers=headers).json()
        state = s["status"]["state"]
        if state in ("PENDING", "RUNNING"):
            time.sleep(1)
            continue
        if state == "FAILED":
            raise RuntimeError(f"SQL execution failed: {s}")
        break

    # Retrieve final result
    result_url = f"{status_url}/result"
    res = requests.get(result_url, headers=headers).json()

    # Extract COUNT(*) from result set
    count = res["results"]["data"][0][0]
    return count


# ------------------------------------------
# Validate table existence via REST API
# ------------------------------------------
def validate_table_exists(host, token, catalog, schema, table):
    url = f"{host}/api/2.1/unity-catalog/tables/{catalog}.{schema}.{table}"
    headers = {"Authorization": f"Bearer {token}"}

    r = requests.get(url, headers=headers)
    return r.status_code == 200


# ------------------------------------------
# Validate all tables
# ------------------------------------------
def validate_tables(host, token, warehouse_id, tables):
    ok = True
    catalog = "dab-mvp-dev"  # DEV ONLY

    for t in tables:
        schema, table = t["name"].split(".", 1)
        min_rows = t.get("min_rows", 0)

        fqdn = f"{catalog}.{schema}.{table}"
        logger.info(f"Checking table: {fqdn}")

        # Existence check
        if not validate_table_exists(host, token, catalog, schema, table):
            logger.error(f"❌ Table not found: {fqdn}")
            ok = False
            continue

        # Row count check
        try:
            count = run_sql(host, token, warehouse_id, catalog, schema, table)
            if count < min_rows:
                logger.error(f"❌ Row count too low for {fqdn}: {count} < {min_rows}")
                ok = False
        except Exception as e:
            logger.error(f"❌ SQL error reading {fqdn}: {e}")
            ok = False

    return ok


# ------------------------------------------
# Validate Jobs
# ------------------------------------------
def validate_jobs(host, token, jobs):
    ok = True
    url = f"{host}/api/2.1/jobs/list"
    headers = {"Authorization": f"Bearer {token}"}

    r = requests.get(url, headers=headers)
    all_jobs = r.json().get("jobs", [])

    for j in jobs:
        expected = j["name"]
        logger.info(f"Checking job: {expected}")

        found = any(job.get("settings", {}).get("name") == expected for job in all_jobs)
        if not found:
            logger.error(f"❌ Job not found: {expected}")
            ok = False

    return ok


# ------------------------------------------
# MAIN
# ------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="validation_config.yml")
    args = parser.parse_args()

    cfg = load_config(args.config)

    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    warehouse_id = os.getenv("WAREHOUSE_ID")

    if not host or not token or not warehouse_id:
        logger.error("❌ Missing environment variables: DATABRICKS_HOST / DATABRICKS_TOKEN / WAREHOUSE_ID")
        sys.exit(1)

    tables_ok = validate_tables(host, token, warehouse_id, cfg.get("tables", []))
    jobs_ok = validate_jobs(host, token, cfg.get("jobs", []))

    if tables_ok and jobs_ok:
        logger.info("✅ All validations passed!")
        sys.exit(0)
    else:
        logger.error("❌ Validation FAILED!")
        sys.exit(1)


if __name__ == "__main__":
    main()
