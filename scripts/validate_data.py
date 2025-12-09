import argparse
import os
import sys
import yaml
import logging
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("validation")


# --------------------------------------------------------
# Load YAML config
# --------------------------------------------------------
def load_config(path):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    root = os.path.dirname(script_dir)
    full_path = os.path.join(root, path)

    if not os.path.exists(full_path):
        raise FileNotFoundError(f"Config file not found: {full_path}")

    with open(full_path, "r") as f:
        return yaml.safe_load(f)


def q(x: str) -> str:
    """Quote identifiers (supports hyphens in catalog names)."""
    return f"`{x}`"


# --------------------------------------------------------
# TABLE EXISTS CHECK (Unity Catalog)
# --------------------------------------------------------
def validate_table_exists(client, catalog, schema, table):
    fqdn = f"{catalog}.{schema}.{table}"
    try:
        client.tables.get(fqdn)
        return True
    except NotFound:
        return False
    except Exception as e:
        logger.error(f"Error checking table: {fqdn} ({e})")
        return False


# --------------------------------------------------------
# ROW COUNT CHECK USING OLD SDK API (0.47 COMPATIBLE)
# --------------------------------------------------------
def validate_row_count(client, warehouse_id, catalog, schema, table, min_rows):
    query = f"SELECT COUNT(*) AS c FROM {q(catalog)}.{q(schema)}.{q(table)}"

    # Submit SQL
    exec_res = client.statement_execution.execute(
        warehouse_id=warehouse_id,
        catalog=catalog,
        schema=schema,
        statement=query,
    )

    statement_id = exec_res.statement_id

    # Poll until done
    status = client.statement_execution.get_statement(statement_id)
    while status.status in ("PENDING", "RUNNING"):
        time.sleep(1)
        status = client.statement_execution.get_statement(statement_id)

    # Fetch full results
    result = client.statement_execution.get_statement_result(statement_id)

    # COUNT(*) lives in: result.manifest.total_row_count == 1 row
    # And data lives in result.result.data_array
    count = result.result.data_array[0][0]

    return count >= min_rows


# --------------------------------------------------------
# VALIDATE ALL TABLES
# --------------------------------------------------------
def validate_tables(client, tables, warehouse_id):
    ok = True
    catalog = "dab-mvp-dev"  # DEV ONLY (as requested)

    for t in tables:
        raw = t["name"]  # schema.table
        min_rows = t.get("min_rows", 0)

        schema, table = raw.split(".", 1)
        fqdn = f"{catalog}.{schema}.{table}"

        logger.info(f"Checking table: {fqdn}")

        # 1. Existence
        if not validate_table_exists(client, catalog, schema, table):
            logger.error(f"❌ Table NOT found: {fqdn}")
            ok = False
            continue

        # 2. Row count
        try:
            if not validate_row_count(client, warehouse_id, catalog, schema, table, min_rows):
                logger.error(f"❌ Row count too low for {fqdn}")
                ok = False
        except Exception as e:
            logger.error(f"❌ Error running row count SQL for {fqdn}: {e}")
            ok = False

    return ok


# --------------------------------------------------------
# VALIDATE JOBS
# --------------------------------------------------------
def validate_jobs(client, jobs):
    ok = True
    all_jobs = list(client.jobs.list())

    for job_cfg in jobs:
        expected = job_cfg["name"]
        logger.info(f"Checking job: {expected}")

        found = any(getattr(j.settings, "name", None) == expected for j in all_jobs)

        if not found:
            logger.error(f"❌ Job NOT found: {expected}")
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
    client = WorkspaceClient()

    warehouse_id = os.getenv("WAREHOUSE_ID")
    if not warehouse_id:
        logger.error("❌ Missing WAREHOUSE_ID environment variable.")
        sys.exit(1)

    tables_ok = validate_tables(client, cfg.get("tables", []), warehouse_id)
    jobs_ok = validate_jobs(client, cfg.get("jobs", []))

    if tables_ok and jobs_ok:
        logger.info("✅ All DEV validations passed!")
        sys.exit(0)
    else:
        logger.error("❌ Validation FAILED.")
        sys.exit(1)


if __name__ == "__main__":
    main()
