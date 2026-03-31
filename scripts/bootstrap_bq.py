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
location = os.getenv("GOOGLE_CLOUD_LOCATION", "us-east1")
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
for sql_file in ["bootstrap.sql", "streaming_setup.sql"]:
    sql_path = os.path.join(ROOT, "sql", sql_file)
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

# --- SEED REALISTIC DATA & PRE-TRAIN BQML ---
try:
    check_q = f"SELECT COUNT(*) as cnt FROM `{project}.{dataset}.simulated_transactions`"
    cnt_df = client.query(check_q).to_dataframe()
    row_count = cnt_df["cnt"].iloc[0] if not cnt_df.empty else 0

    if row_count == 0:
        print("Table is cold. Seeding 20 realistic transactions for BQML training...")
        import uuid
        import random
        from datetime import datetime
        
        cardholders = [
            {"id": "CH-1001", "limit": 5000},
            {"id": "CH-1002", "limit": 2000},
            {"id": "CH-1003", "limit": 10000},
            {"id": "CH-1004", "limit": 1500},
        ]
        
        rows = []
        for _ in range(25):
            ch = random.choice(cardholders)
            is_breach = random.random() < 0.3
            amount = random.randint(100, 2000)
            if is_breach:
                amount = ch["limit"] + random.randint(100, 500)
            
            rows.append({
                "transaction_id": f"TX-{uuid.uuid4().hex[:10].upper()}",
                "cardholder_id": ch["id"],
                "transaction_amount": amount,
                "credit_limit": ch["limit"],
                "transaction_time": datetime.utcnow().isoformat() + "Z",
                "is_fraud_label": 1 if is_breach else 0,
            })
        
        table_id = f"{project}.{dataset}.simulated_transactions"
        errors = client.insert_rows_json(table_id, rows)
        if errors:
            print(f"Errors seeding rows: {errors}")
        else:
            print("✅ Seeding complete!")

        # Train the model now!
        print("Running pre-train BQML model...")
        train_sql_path = os.path.join(ROOT, "sql", "train_fraud_model.sql")
        with open(train_sql_path, "r") as f:
            train_raw = f.read()
        train_rendered = train_raw.replace("{{project}}", project).replace("{{dataset}}", dataset)
        client.query(train_rendered).result()
        print("✅ BQML Fraud Model Pre-trained successfully on deploy!")

except Exception as e:
    print(f"Error during data seeding/model training: {e}")

print(f"Bootstrapped BigQuery objects in {project}.{dataset}")
