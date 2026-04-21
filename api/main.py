import os
import sys
from typing import Any

from fastapi import FastAPI, File, Form, UploadFile

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from rwa_demo.workflow import DemoWorkflowService

app = FastAPI(title="RWA Demo API", version="1.0.0")
_service: DemoWorkflowService | None = None


def get_service() -> DemoWorkflowService:
    global _service
    if _service is None:
        _service = DemoWorkflowService()
    return _service


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/policy/upload")
async def upload_policy(
    uploaded_by: str = Form(...),
    existing_policy_id: str | None = Form(default=None),
    supersedes_policy_version_id: str | None = Form(default=None),
    file: UploadFile = File(...),
) -> dict[str, Any]:
    file_bytes = await file.read()
    policy_id, policy_version_id, gcs_uri = get_service().upload_policy(
        uploaded_by=uploaded_by,
        filename=file.filename or "policy.pdf",
        file_bytes=file_bytes,
        existing_policy_id=existing_policy_id,
        supersedes_policy_version_id=supersedes_policy_version_id,
    )
    return {
        "policy_id": policy_id,
        "policy_version_id": policy_version_id,
        "gcs_uri": gcs_uri,
    }


@app.post("/sql/generate")
def generate_sql(payload: dict[str, str]) -> dict[str, str]:
    policy_version_id = payload["policy_version_id"]
    sql_version_id, summary, generated_sql = get_service().generate_sql(policy_version_id=policy_version_id)
    return {
        "policy_version_id": policy_version_id,
        "sql_version_id": sql_version_id,
        "summary": summary,
        "generated_sql": generated_sql,
    }


@app.post("/sql/approve")
def approve_sql(payload: dict[str, str]) -> dict[str, str]:
    get_service().approve_sql(
        sql_version_id=payload["sql_version_id"],
        approved_by=payload["approved_by"],
    )
    return {"status": "approved", "sql_version_id": payload["sql_version_id"]}


@app.post("/sql/execute")
def execute_sql(payload: dict[str, str]) -> dict[str, str]:
    run_id = get_service().execute_sql_agent(
        policy_id=payload["policy_id"],
        policy_version_id=payload["policy_version_id"],
        sql_version_id=payload["sql_version_id"],
    )
    return {"status": "succeeded", "run_id": run_id}


@app.get("/metrics")
def get_metrics() -> dict[str, Any]:
    return get_service().repo.get_dashboard_metrics()


@app.get("/policies/{policy_id}/timeline")
def get_policy_timeline(policy_id: str) -> list[dict[str, Any]]:
    return get_service().repo.get_policy_timeline(policy_id)


@app.get("/policies/{policy_id}/sql")
def get_sql_versions(policy_id: str) -> list[dict[str, Any]]:
    return get_service().repo.get_sql_versions_for_policy(policy_id)


@app.get("/runs")
def list_runs() -> list[dict[str, Any]]:
    return get_service().repo.list_report_runs()


@app.get("/comparison")
def get_comparison(baseline_run: str, updated_run: str) -> list[dict[str, Any]]:
    return get_service().repo.get_rwa_comparison(baseline_run, updated_run)


@app.get("/schema-check/{sql_version_id}")
def check_schema_drift(sql_version_id: str) -> dict[str, Any]:
    return get_service().repo.get_schema_drift(sql_version_id)

