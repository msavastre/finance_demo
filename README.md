# RWA Policy-to-SQL Demo (Google Native)

End-to-end demo for Global Finance/Treasury regulatory reporting:

- Upload policy PDF
- Store policy metadata in BigQuery with `OBJECT_REF`
- Generate SQL from policy using an agent
- Approve and execute SQL with an execution agent
- Persist versioned report outputs and lineage
- Optional cloud deployment with Cloud Run + Cloud Workflows

## Quick start

1. Create a virtual environment and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Configure environment:

```bash
cp .env.example .env
# Edit .env with your GCP project details
```

3. Bootstrap BigQuery assets:

```bash
python scripts/bootstrap_bq.py
```

4. Run UI:

```bash
streamlit run ui/streamlit_app.py
```

## Cloud deployment

### Deploy API to Cloud Run

```bash
export GOOGLE_CLOUD_PROJECT=your-project-id
export GOOGLE_CLOUD_LOCATION=us-central1
bash deploy/deploy_api.sh
```

### Deploy UI to Cloud Run

```bash
export GOOGLE_CLOUD_PROJECT=your-project-id
export GOOGLE_CLOUD_LOCATION=us-central1
bash deploy/deploy_ui.sh
```

### Deploy Cloud Workflow

```bash
export GOOGLE_CLOUD_PROJECT=your-project-id
export GOOGLE_CLOUD_LOCATION=us-central1
bash deploy/deploy_workflow.sh
```

### Execute workflow

```bash
gcloud workflows run rwa-policy-pipeline \
  --location us-central1 \
  --data '{
    "api_base_url":"https://<api-cloud-run-url>",
    "policy_id":"POL-EXAMPLE001",
    "policy_version_id":"POL-EXAMPLE001-V0001",
    "approved_by":"finance.lead@hsbc.com"
  }'
```

## Notes on OBJECT_REF

The `policy_documents.policy_object_ref` column uses BigQuery `OBJECT_REF`.
This demo creates refs via `OBJ.MAKE_REF(<gs://...>)`.

If your BigQuery environment requires a different object reference function/syntax,
update `BigQueryRepository.register_policy_document`.

## Demo workflow

1. Upload baseline policy PDF
2. Generate policy interpretation + SQL
3. Approve SQL
4. Execute SQL agent and store output version
5. Upload updated policy and repeat
6. Compare report outputs by `policy_version_id`

## Agentic SQL generation behavior

- SQL is generated non-deterministically by Vertex Gemini from:
  - the uploaded policy PDF in GCS
  - live BigQuery schema snapshot from `INFORMATION_SCHEMA`
- Generated SQL metadata stores:
  - `agent_trace` (model, citations, generation timestamp)
  - `schema_snapshot` (tables/columns/types used for grounding)
