# RWA Policy-to-SQL Demo (Google Native)

End-to-end demo for Global Finance/Treasury regulatory reporting. Converts internal RWA policy PDFs into versioned, auditable BigQuery SQL and report outputs — with full lineage from policy document to regulatory dataset.

## What it does

- Upload policy PDFs → store in Cloud Storage with BigQuery `OBJECT_REF` metadata
- Vertex AI (Gemini) extracts policy rules and generates grounded BigQuery SQL
- Human approval gate before SQL execution
- Versioned RWA report outputs stamped with policy/sql/run IDs
- Full audit lineage: policy PDF → extraction → SQL → run → outputs

## UI Overview (9 tabs)

| Tab | Purpose |
|-----|---------|
| **1) Upload Policy** | Upload baseline or updated PDF; auto-registers policy_id and version |
| **2) Generate SQL** | Streaming AI generation with phased progress; clause citation display; schema drift check |
| **3) Approve & Execute** | SQL approval gate + agent-driven SQL execution against BigQuery |
| **4) SQL Diff** | GitHub-style side-by-side diff between any two SQL versions for a policy |
| **5) Impact Dashboard** | Grouped bar + waterfall charts comparing RWA between runs; stress test overlay (1x–3x) |
| **6) Explainability** | Clause-to-SQL mapping: policy clauses linked to SQL sections with colour-coded badges |
| **7) Lineage & Audit** | Policy version timeline; Mermaid lineage graph; one-click audit package export (.zip) |
| **8) Capital Ratios** | CET1, Tier 1 and Total Capital gauge indicators vs Basel III minimums; capital buffer analysis |
| **9) RWA Analyst** | Natural language Gemini queries grounded on session RWA data |

### Demo Mode

Toggle **Demo Mode** in the sidebar to activate a guided 8-step walkthrough (Acts 1–3) that auto-fills IDs between tabs — no copy-pasting UUIDs during a live presentation.

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

3. Bootstrap BigQuery tables and seed data:

```bash
python scripts/bootstrap_bq.py
```

4. Run the UI:

```bash
streamlit run ui/streamlit_app.py
```

## Demo narrative (3-act structure)

**Act 1 — Baseline:** Upload baseline policy PDF → generate SQL (watch streaming AI) → approve → execute → view traceability in Tab 7.

**Act 2 — Policy update:** Upload revised PDF → SQL auto-updates → run new version → compare RWA delta in Tab 5 → apply stress test slider → check capital ratios in Tab 8.

**Act 3 — Governance:** Open Tab 7 to walk the Mermaid lineage graph → export audit package → ask Gemini a question in Tab 9 to explain the RWA movement.

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
    "approved_by":"finance.lead@bank.com"
  }'
```

## Architecture notes

### BigQuery OBJECT_REF

The `policy_documents.policy_object_ref` column uses BigQuery `OBJECT_REF` created via `OBJ.MAKE_REF(<gs://...>)`. If your environment requires different syntax, update `BigQueryRepository.register_policy_document`.

### Agentic SQL generation

SQL is generated non-deterministically by Vertex Gemini from:
- the uploaded policy PDF read directly from GCS
- a live BigQuery schema snapshot from `INFORMATION_SCHEMA`

Each generated SQL version stores:
- `agent_trace` — model ID, clause citations, generation timestamp
- `schema_snapshot` — tables/columns/types at generation time (used for schema drift detection)

### Schema drift detection

Tab 2 compares the `schema_snapshot` stored at SQL generation time against the current live schema. Columns added or removed since generation are surfaced as warnings before re-running.

### Stress testing

Tab 5 applies a configurable multiplier (1x–3x) to updated RWA figures to simulate stressed market conditions and computes the additional capital required at the 8% Basel III Pillar 1 floor.

### Audit package

Tab 7 exports a `.zip` containing: lineage metadata JSON, generated SQL file, RWA comparison CSV, Mermaid lineage graph source, and a run summary JSON — ready for regulatory or internal audit submission.
