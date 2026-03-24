import os
import sys

from google.cloud import bigquery
from google.cloud import bigquery_connection_v1
from google.cloud import storage
from google.api_core.exceptions import AlreadyExists, NotFound
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

# ── 1. Create BigQuery dataset ────────────────────────────────────────────────
client = bigquery.Client(project=project)
client.query(f"CREATE SCHEMA IF NOT EXISTS `{project}.{dataset}`").result()
print(f"Dataset ready: {project}.{dataset}")

# ── 2. Create Cloud Resource connection for Object Table access ───────────────
conn_client = bigquery_connection_v1.ConnectionServiceClient()
parent = f"projects/{project}/locations/{location}"
conn_name = f"{parent}/connections/gcs-connection"

try:
    existing = conn_client.get_connection(name=conn_name)
    service_account = existing.cloud_resource.service_account_id
    print(f"BQ connection already exists, SA: {service_account}")
except NotFound:
    connection = bigquery_connection_v1.Connection(
        cloud_resource=bigquery_connection_v1.CloudResourceProperties()
    )
    response = conn_client.create_connection(
        parent=parent,
        connection_id="gcs-connection",
        connection=connection,
    )
    service_account = response.cloud_resource.service_account_id
    print(f"Created BQ connection, SA: {service_account}")

# ── 3. Grant the connection SA Storage Object Viewer on the policy bucket ─────
storage_client = storage.Client(project=project)
bucket_obj = storage_client.bucket(policy_bucket)
iam_policy = bucket_obj.get_iam_policy(requested_policy_version=3)
member = f"serviceAccount:{service_account}"
role = "roles/storage.objectViewer"
already_granted = any(
    b.get("role") == role and member in b.get("members", set())
    for b in iam_policy.bindings
)
if not already_granted:
    iam_policy.bindings.append({"role": role, "members": {member}})
    bucket_obj.set_iam_policy(iam_policy)
    print(f"Granted {role} to {member} on gs://{policy_bucket}")
else:
    print(f"IAM already set for {member} on gs://{policy_bucket}")

# ── 4. Run SQL bootstrap ───────────────────────────────────────────────────────
sql_path = os.path.join(ROOT, "sql", "bootstrap.sql")
with open(sql_path, "r", encoding="utf-8") as f:
    raw = f.read()

rendered = (
    raw.replace("{{project}}", project)
       .replace("{{dataset}}", dataset)
       .replace("{{location}}", location)
       .replace("{{bucket}}", policy_bucket)
)

for statement in [s.strip() for s in rendered.split(";") if s.strip()]:
    client.query(statement).result()

print(f"Bootstrapped BigQuery objects in {project}.{dataset}")
