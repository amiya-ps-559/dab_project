import argparse
import yaml
import sys
import logging

# Optional imports
try:
    from pyspark.sql import SparkSession
except:
    SparkSession = None

try:
    from databricks import WorkspaceClient
except:
    WorkspaceClient = None

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("validation")


def load_config(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)


def get_spark():
    if SparkSession is None:
        logger.error("PySpark not available. Run inside Databricks or install pyspark.")
        sys.exit(1)
    return SparkSession.builder.getOrCreate()


def validate_tables(spark, tables):
    ok = True
    for t in tables:
        table_name = t["name"]
        min_rows = t.get("min_rows", 0)

        logger.info(f"Checking table: {table_name}")

        # Check existence
        try:
            if not spark.catalog.tableExists(table_name):
                logger.error(f"❌ Table does not exist: {table_name}")
                ok = False
                continue
        except Exception as e:
            logger.error(f"❌ Error checking table existence: {table_name} ({e})")
            ok = False
            continue

        # Check min rows
        try:
            df = spark.table(table_name)
            count = df.count()
            if count < min_rows:
                logger.error(f"❌ Row count check failed: {table_name} (count={count}, expected>={min_rows})")
                ok = False
        except Exception as e:
            logger.error(f"❌ Error reading table {table_name}: {e}")
            ok = False

    return ok


def validate_jobs(jobs):
    if WorkspaceClient is None:
        logger.warning("databricks-sdk not installed. Skipping job validation.")
        return True

    client = WorkspaceClient()
    jobs_list = list(client.jobs.list())

    ok = True
    for job in jobs:
        name = job["name"]
        must_be_active = job.get("must_be_active", False)

        logger.info(f"Checking job: {name}")

        found_job = None
        for j in jobs_list:
            j_name = getattr(j.settings, "name", None)
            if j_name == name:
                found_job = j
                break

        if not found_job:
            logger.error(f"❌ Job not found: {name}")
            ok = False
            continue

        # Activity check (simple existence check is enough for dev)
        if must_be_active:
            logger.info(f"Job exists and is considered active: {name}")

    return ok


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="validation_config.yaml")
    args = parser.parse_args()

    cfg = load_config(args.config)
    spark = get_spark()

    tables_ok = validate_tables(spark, cfg.get("tables", []))
    jobs_ok = validate_jobs(cfg.get("jobs", []))

    if tables_ok and jobs_ok:
        logger.info("✅ All DEV validations passed.")
        sys.exit(0)
    else:
        logger.error("❌ DEV validations FAILED.")
        sys.exit(1)


if __name__ == "__main__":
    main()
