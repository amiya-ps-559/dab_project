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
# Load YAML Config
# --------------------------------------------------------
def load_config(path):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    full_path = os.path.join(project_root, path)

    if not os.path.exists(full_path):
        raise FileNotFoundError(f"Config file not found: {full_path}")

    with open(full_path, "r") as f:
        return yaml.safe_load(f)


def q(x: str) -> str:
    """Quote identifiers for UC SQL."""
    return f"`{x}`"


# --------------------------------------------------------
# Validate Table Exists
# --------------------------------------------------------
def validate_table_exists(client, catalog, schema, table):
    fqdn = f"{catalog}.{schema}.{table}"
    try:
        client.tables.get(fqdn)
        return True
    except NotFound:
        return False
    except Exception as e:
        logger.error(f"Error checking table existence via API: {fqdn} ({e})")
        return False


# --------------------------------------------------------
# Validate Row Count
# --------------------------------------------------------
def validate_row_count(client, warehouse_id, catalog, schema, table, min_rows):
    query = f"SELECT COUNT(*) AS c FROM {q(catalog)}.{q(schema)}.{q(table)}"

    exec_res = client.statement_execution.execute(
        warehouse_id=warehouse_id,
        catalog=catalog,
        schema=schema,
        statement=query,
    )

    # Wait for execution to finish
    while exec_res.status == "PENDING" or exec_res.status == "RUNNING":
        time.sleep(1)
        exec_res = client.statement_execution.get_statement(exec_res.statement_id)

    # Get results
    result = client.statement_execution.get_statement_result(exec_res.statement_id)
    count = result.manifest.total_row_count

    return count >= min_rows


# --------------------------------------------------------
# Validate Tables Section
# --------------------------------------------------------
def validate_tables(client, tables, warehouse_id):
    ok = True
    catalog = "dab-mvp-dev"  # DEV ONLY as you requested

    for t in tables:
        raw = t["name"]  # schema.table
        min_rows = t.get("min_rows", 0)

        schema, table = raw.split(".", 1)
        fqdn = f"{catalog}.{schema}.{table}"

        logger.info(f"Checking table: {fqdn}")

        # Existence
        if not validate_table_exists(client, catalog, schema, table):
            logger.error(f"❌ Table does NOT exist: {fqdn}")
            ok = False
            continue

        # Row count
        try:
            if not validate_row_count(client, warehouse_id, catalog, schema, table, min_rows):
                logger.error(f"❌ Row count too low for: {fqdn}")
                ok = False
        except Exception as e:
            logger.error(f"❌ Error in row count check for {fqdn}: {e}")
            ok = False

    return ok


# --------------------------------------------------------
# Validate Jobs Section
# --------------------------------------------------------
def validate_jobs(client, jobs):
    ok = True
    all_jobs = list(client.jobs.list())

    for j in jobs:
        expected_name = j["name"]
        logger.info(f"Checking job: {expected_name}")

        found = any(getattr(job.settings, "name", None) == expected_name for job in all_jobs)

        if not found:
            logger.error(f"❌ Job not found: {expected_name}")
            ok = False

    return ok


# --------------------------------------------------------
# MAIN ENTRYPOINT
# --------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="validation_config.yml")
    args = parser.parse_args()

    cfg = load_config(args.config)

    # Databricks client
    client = WorkspaceClient()

    # Warehouse ID required for SQL API
    warehouse_id = os.getenv("WAREHOUSE_ID")
    if not warehouse_id:
        logger.error("❌ WAREHOUSE_ID environment variable is required.")
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
