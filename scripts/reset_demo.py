"""
Reset the demo BigQuery dataset and GCS bucket to a clean pre-demo state.

What this does:
  1. Deletes all rows from transactional tables (policy_documents, policy_extractions,
     policy_sql_versions, report_runs, rwa_report_outputs)
  2. Truncates and re-seeds reference tables (exposures, risk_weight_mapping)
  3. Deletes all objects from the GCS policy bucket
  4. Leaves schema, connections, and Object Table intact

Run with:
  python scripts/reset_demo.py

Add --hard to also drop and recreate the entire dataset (full schema reset):
  python scripts/reset_demo.py --hard
"""

import os
import sys
import argparse

from google.cloud import bigquery, storage
from dotenv import load_dotenv

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
load_dotenv(os.path.join(ROOT, ".env"))

project = os.getenv("GOOGLE_CLOUD_PROJECT")
dataset = os.getenv("BIGQUERY_DATASET", "finance_demo")
location = os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1")
policy_bucket = os.getenv("GCS_POLICY_BUCKET", "")

if not project:
    raise SystemExit("GOOGLE_CLOUD_PROJECT is required in .env")
if not policy_bucket:
    raise SystemExit("GCS_POLICY_BUCKET is required in .env")

parser = argparse.ArgumentParser()
parser.add_argument("--hard", action="store_true",
                    help="Drop and recreate the entire dataset (full schema reset)")
args = parser.parse_args()

bq = bigquery.Client(project=project)
gcs = storage.Client(project=project)


def run(sql: str) -> None:
    bq.query(sql).result()


def t(table: str) -> str:
    return f"`{project}.{dataset}.{table}`"


if args.hard:
    print(f"Hard reset: dropping dataset {project}.{dataset} ...")
    bq.delete_dataset(f"{project}.{dataset}", delete_contents=True, not_found_ok=True)
    print("Dataset dropped. Re-run bootstrap_bq.py to recreate schema.")
    print("  python scripts/bootstrap_bq.py")
else:
    print(f"Soft reset: truncating transactional tables in {project}.{dataset} ...")

    # Truncate all transactional tables
    for table in [
        "policy_documents",
        "policy_extractions",
        "policy_sql_versions",
        "report_runs",
        "rwa_report_outputs",
    ]:
        run(f"TRUNCATE TABLE {t(table)}")
        print(f"  Truncated {table}")

    # Re-seed reference tables
    run(f"TRUNCATE TABLE {t('exposures')}")
    run(f"""
INSERT INTO {t('exposures')} (portfolio, asset_class, exposure_amount)
SELECT * FROM UNNEST([
  STRUCT('Treasury_Book' AS portfolio, 'Sovereign' AS asset_class, NUMERIC '20000000' AS exposure_amount),
  STRUCT('Treasury_Book', 'Bank', NUMERIC '12000000'),
  STRUCT('Markets_Book', 'Corporate', NUMERIC '15000000')
])
""")
    print("  Re-seeded exposures")

    run(f"TRUNCATE TABLE {t('risk_weight_mapping')}")
    run(f"""
INSERT INTO {t('risk_weight_mapping')} (asset_class, risk_bucket, risk_weight)
SELECT * FROM UNNEST([
  STRUCT('Sovereign' AS asset_class, 'Low' AS risk_bucket, NUMERIC '0.20' AS risk_weight),
  STRUCT('Bank', 'Medium', NUMERIC '0.50'),
  STRUCT('Corporate', 'High', NUMERIC '1.00')
])
""")
    print("  Re-seeded risk_weight_mapping")

# Delete all objects from the GCS policy bucket
print(f"Deleting all objects from gs://{policy_bucket} ...")
bucket_obj = gcs.bucket(policy_bucket)
blobs = list(bucket_obj.list_blobs())
if blobs:
    bucket_obj.delete_blobs(blobs)
    print(f"  Deleted {len(blobs)} object(s)")
else:
    print("  Bucket already empty")

print("\nDemo reset complete. Ready for a fresh run.")
