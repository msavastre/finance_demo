import json
from typing import Any

from google.cloud import bigquery

from rwa_demo.config import settings


class BigQueryRepository:
    def __init__(self) -> None:
        self.client = bigquery.Client(project=settings.project_id)
        self.project = settings.project_id
        self.dataset = settings.dataset

    def _table(self, table: str) -> str:
        return f"`{self.project}.{self.dataset}.{table}`"

    def register_policy_document(
        self,
        policy_id: str,
        policy_version_id: str,
        uploaded_by: str,
        gcs_uri: str,
        supersedes_policy_version_id: str | None = None,
    ) -> None:
        query = f"""
INSERT INTO {self._table("policy_documents")}
(
  policy_id,
  policy_version_id,
  uploaded_at,
  uploaded_by,
  gcs_uri,
  policy_object_ref,
  status,
  supersedes_policy_version_id
)
VALUES
(
  @policy_id,
  @policy_version_id,
  CURRENT_TIMESTAMP(),
  @uploaded_by,
  @gcs_uri,
  OBJ.MAKE_REF(@gcs_uri),
  'draft',
  @supersedes_policy_version_id
)
"""
        params = [
            bigquery.ScalarQueryParameter("policy_id", "STRING", policy_id),
            bigquery.ScalarQueryParameter("policy_version_id", "STRING", policy_version_id),
            bigquery.ScalarQueryParameter("uploaded_by", "STRING", uploaded_by),
            bigquery.ScalarQueryParameter("gcs_uri", "STRING", gcs_uri),
            bigquery.ScalarQueryParameter(
                "supersedes_policy_version_id", "STRING", supersedes_policy_version_id
            ),
        ]
        self.client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()

    def save_policy_extraction(self, policy_version_id: str, extraction_summary: str, extraction_json: str) -> None:
        query = f"""
INSERT INTO {self._table("policy_extractions")}
(policy_version_id, extraction_json, clause_citations, model_version, created_at)
VALUES (@policy_version_id, PARSE_JSON(@extraction_json), PARSE_JSON(@clause_citations), @model_version, CURRENT_TIMESTAMP())
"""
        params = [
            bigquery.ScalarQueryParameter("policy_version_id", "STRING", policy_version_id),
            bigquery.ScalarQueryParameter("extraction_json", "STRING", extraction_json),
            bigquery.ScalarQueryParameter(
                "clause_citations",
                "STRING",
                '{"summary": "' + extraction_summary.replace('"', '\\"') + '"}',
            ),
            bigquery.ScalarQueryParameter("model_version", "STRING", "gemini-demo-template-v1"),
        ]
        self.client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()

    def save_generated_sql(
        self,
        sql_version_id: str,
        policy_version_id: str,
        generated_sql: str,
        agent_trace: dict[str, Any],
        schema_snapshot: dict[str, Any],
    ) -> None:
        query = f"""
INSERT INTO {self._table("policy_sql_versions")}
(sql_version_id, policy_version_id, generated_sql, agent_trace, schema_snapshot, validation_status, approved_by, approved_at)
VALUES (
  @sql_version_id,
  @policy_version_id,
  @generated_sql,
  PARSE_JSON(@agent_trace),
  PARSE_JSON(@schema_snapshot),
  'pending',
  NULL,
  NULL
)
"""
        params = [
            bigquery.ScalarQueryParameter("sql_version_id", "STRING", sql_version_id),
            bigquery.ScalarQueryParameter("policy_version_id", "STRING", policy_version_id),
            bigquery.ScalarQueryParameter("generated_sql", "STRING", generated_sql),
            bigquery.ScalarQueryParameter("agent_trace", "STRING", json.dumps(agent_trace)),
            bigquery.ScalarQueryParameter("schema_snapshot", "STRING", json.dumps(schema_snapshot)),
        ]
        self.client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()

    def approve_sql(self, sql_version_id: str, approved_by: str) -> None:
        query = f"""
UPDATE {self._table("policy_sql_versions")}
SET validation_status = 'approved',
    approved_by = @approved_by,
    approved_at = CURRENT_TIMESTAMP()
WHERE sql_version_id = @sql_version_id
"""
        params = [
            bigquery.ScalarQueryParameter("approved_by", "STRING", approved_by),
            bigquery.ScalarQueryParameter("sql_version_id", "STRING", sql_version_id),
        ]
        self.client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()

    def create_report_run(self, run_id: str, policy_id: str, policy_version_id: str, sql_version_id: str) -> None:
        query = f"""
INSERT INTO {self._table("report_runs")}
(run_id, policy_id, policy_version_id, sql_version_id, run_status, started_at, ended_at)
VALUES (@run_id, @policy_id, @policy_version_id, @sql_version_id, 'running', CURRENT_TIMESTAMP(), NULL)
"""
        params = [
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
            bigquery.ScalarQueryParameter("policy_id", "STRING", policy_id),
            bigquery.ScalarQueryParameter("policy_version_id", "STRING", policy_version_id),
            bigquery.ScalarQueryParameter("sql_version_id", "STRING", sql_version_id),
        ]
        self.client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()

    def complete_report_run(self, run_id: str, status: str) -> None:
        query = f"""
UPDATE {self._table("report_runs")}
SET run_status = @status, ended_at = CURRENT_TIMESTAMP()
WHERE run_id = @run_id
"""
        params = [
            bigquery.ScalarQueryParameter("status", "STRING", status),
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
        ]
        self.client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()

    def execute_sql(self, query: str) -> None:
        self.client.query(query).result()

    def get_latest_generated_sql(self, sql_version_id: str) -> str | None:
        query = f"""
SELECT generated_sql
FROM {self._table("policy_sql_versions")}
WHERE sql_version_id = @sql_version_id
LIMIT 1
"""
        params = [bigquery.ScalarQueryParameter("sql_version_id", "STRING", sql_version_id)]
        rows = list(
            self.client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
        )
        return rows[0]["generated_sql"] if rows else None

    def list_table(self, table: str, limit: int = 100) -> list[dict[str, Any]]:
        query = f"SELECT * FROM {self._table(table)} ORDER BY 1 DESC LIMIT @limit"
        params = [bigquery.ScalarQueryParameter("limit", "INT64", limit)]
        rows = self.client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
        return [dict(row) for row in rows]

    def get_policy_gcs_uri(self, policy_version_id: str) -> str:
        query = f"""
SELECT gcs_uri
FROM {self._table("policy_documents")}
WHERE policy_version_id = @policy_version_id
LIMIT 1
"""
        params = [bigquery.ScalarQueryParameter("policy_version_id", "STRING", policy_version_id)]
        rows = list(
            self.client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
        )
        if not rows:
            raise ValueError(f"No policy document found for {policy_version_id}")
        return rows[0]["gcs_uri"]

    def get_schema_snapshot(self) -> dict[str, Any]:
        query = f"""
SELECT table_name, column_name, data_type
FROM `{self.project}.{self.dataset}.INFORMATION_SCHEMA.COLUMNS`
ORDER BY table_name, ordinal_position
"""
        rows = list(self.client.query(query).result())
        tables: dict[str, list[dict[str, str]]] = {}
        for row in rows:
            table = row["table_name"]
            tables.setdefault(table, []).append(
                {
                    "column_name": row["column_name"],
                    "data_type": row["data_type"],
                }
            )
        return {"project": self.project, "dataset": self.dataset, "tables": tables}

