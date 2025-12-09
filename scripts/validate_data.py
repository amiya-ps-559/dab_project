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


def load_config(path):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    full_path = os.path.join(project_root, path)

    if not os.path.exists(full_path):
        raise FileNotFoundError(f"Config file not found: {full_path}")

    with open(full_path, "r") as f:
        return yaml.safe_load(f)


def q(x: str) -> str:
    return f"`{x}`"


def validate_table_exists(client, catalog, schema, table):
    fqdn = f"{catalog}.{schema}.{table}"
    try:
        client.tables.get(fqdn)
        return True
    except NotFound:
        return False
    except Exception as e:
        logger.error(f"Error checking table existence: {fqdn} ({e})")
        return False


def validate_row_count(client, warehouse_id, catalog, schema, table, min_rows):
    query = f"SELECT COUNT(*) AS c FROM {q(catalog)}.{q(schema)}.{q(table)}"

    exec_res = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        catalog=catalog,
        schema=schema,
        statement=query,
    )

    statement_id = exec_res.statement_id

    # Poll until the statement finishes
    status = client.statement_execution.get_statement(statement_id)
    while status.status in ("PENDING", "RUNNING"):
        time.sleep(1)
        status = client.statement_execution.get_statement(statement_id)

    # Fetch first result chunk
    chunk = client.statement_execution.get_result_chunk_n(statement_id, 0)

    # COUNT(*) = row 0 col 0
    count = chunk.data_array[0][0]

    return count >= min_rows


def validate_tables(client, tables, warehouse_id):
    ok = True
    catalog = "dab-mvp-dev"  # Hard-coded for DEV

    for t in tables:
        raw = t["name"]
        min_rows = t.get("min_rows", 0)
        schema, table = raw.split(".", 1)

        fqdn = f"{catalog}.{schema}.{table}"
        logger.info(f"Checking table: {fqdn}")

        if not validate_table_exists(client, catalog, schema, table):
            logger.error(f"❌ Table does NOT exist: {fqdn}")
            ok = False
            continue

        try:
            if not validate_row_count(client, warehouse_id, catalog, schema, table, min_rows):
                logger.error(f"❌ Row count too low for: {fqdn}")
                ok = False
        except Exception as e:
            logger.error(f"❌ Error during row count validation for {fqdn}: {e}")
            ok = False

    return ok


def validate_jobs(client, jobs):
    ok = True
    all_jobs = list(client.jobs.list())

    for j in jobs:
        expected = j["name"]
        logger.info(f"Checking job: {expected}")

        found = any(getattr(job.settings, "name", None) == expected for job in all_jobs)

        if not found:
            logger.error(f"❌ Job not found: {expected}")
            ok = False

    return ok


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="validation_config.yml")
    args = parser.parse_args()

    cfg = load_config(args.config)

    client = WorkspaceClient()

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
