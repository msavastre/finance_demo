import os
import sys

from google.cloud import bigquery
from dotenv import load_dotenv

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
load_dotenv(os.path.join(ROOT, ".env"))

project = os.getenv("GOOGLE_CLOUD_PROJECT")
dataset = os.getenv("BIGQUERY_DATASET", "rwa_demo")

if not project:
    raise SystemExit("GOOGLE_CLOUD_PROJECT is required in .env")

client = bigquery.Client(project=project)
client.query(f"CREATE SCHEMA IF NOT EXISTS `{project}.{dataset}`").result()

sql_path = os.path.join(ROOT, "sql", "bootstrap.sql")
with open(sql_path, "r", encoding="utf-8") as f:
    raw = f.read()

rendered = raw.replace("{{project}}", project).replace("{{dataset}}", dataset)

for statement in [s.strip() for s in rendered.split(";") if s.strip()]:
    client.query(statement).result()

print(f"Bootstrapped BigQuery objects in {project}.{dataset}")

