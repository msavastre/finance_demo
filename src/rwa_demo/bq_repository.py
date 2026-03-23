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

    def get_dashboard_metrics(self) -> dict[str, Any]:
        query = f"""
SELECT
  (SELECT COUNT(*) FROM {self._table("policy_documents")}) AS total_policies,
  (SELECT COUNT(*) FROM {self._table("policy_sql_versions")} WHERE validation_status = 'approved') AS approved_sql,
  (SELECT COUNT(*) FROM {self._table("report_runs")} WHERE run_status = 'succeeded') AS successful_runs,
  (SELECT COALESCE(SUM(rwa_amount), 0) FROM {self._table("rwa_report_outputs")}) AS total_rwa
"""
        rows = list(self.client.query(query).result())
        if rows:
            row = rows[0]
            return {
                "total_policies": row["total_policies"],
                "approved_sql": row["approved_sql"],
                "successful_runs": row["successful_runs"],
                "total_rwa": float(row["total_rwa"]),
            }
        return {"total_policies": 0, "approved_sql": 0, "successful_runs": 0, "total_rwa": 0.0}

    def list_report_runs(self) -> list[dict[str, Any]]:
        query = f"""
SELECT run_id, policy_id, policy_version_id, sql_version_id, run_status, started_at
FROM {self._table("report_runs")}
ORDER BY started_at DESC
LIMIT 50
"""
        return [dict(row) for row in self.client.query(query).result()]

    def get_rwa_comparison(self, run_id_a: str, run_id_b: str) -> list[dict[str, Any]]:
        query = f"""
SELECT
  COALESCE(a.portfolio, b.portfolio) AS portfolio,
  COALESCE(a.risk_bucket, b.risk_bucket) AS risk_bucket,
  IFNULL(a.rwa_amount, 0) AS rwa_baseline,
  IFNULL(b.rwa_amount, 0) AS rwa_updated,
  IFNULL(b.rwa_amount, 0) - IFNULL(a.rwa_amount, 0) AS rwa_delta
FROM (
  SELECT portfolio, risk_bucket, SUM(rwa_amount) AS rwa_amount
  FROM {self._table("rwa_report_outputs")}
  WHERE run_id = @run_id_a
  GROUP BY portfolio, risk_bucket
) a
FULL OUTER JOIN (
  SELECT portfolio, risk_bucket, SUM(rwa_amount) AS rwa_amount
  FROM {self._table("rwa_report_outputs")}
  WHERE run_id = @run_id_b
  GROUP BY portfolio, risk_bucket
) b
ON a.portfolio = b.portfolio AND a.risk_bucket = b.risk_bucket
ORDER BY portfolio, risk_bucket
"""
        params = [
            bigquery.ScalarQueryParameter("run_id_a", "STRING", run_id_a),
            bigquery.ScalarQueryParameter("run_id_b", "STRING", run_id_b),
        ]
        return [dict(row) for row in self.client.query(
            query, job_config=bigquery.QueryJobConfig(query_parameters=params)
        ).result()]

    def get_lineage_chain(self, run_id: str) -> dict[str, Any] | None:
        query = f"""
SELECT
  pd.policy_id,
  pd.policy_version_id,
  pd.gcs_uri,
  pd.uploaded_by,
  pd.uploaded_at,
  pd.status AS policy_status,
  pe.model_version,
  pe.created_at AS extraction_at,
  JSON_VALUE(pe.clause_citations, '$.summary') AS extraction_summary,
  psv.sql_version_id,
  psv.validation_status,
  psv.approved_by,
  psv.approved_at,
  rr.run_id,
  rr.run_status,
  rr.started_at AS run_started,
  rr.ended_at AS run_ended
FROM {self._table("report_runs")} rr
JOIN {self._table("policy_documents")} pd
  ON rr.policy_id = pd.policy_id AND rr.policy_version_id = pd.policy_version_id
LEFT JOIN {self._table("policy_extractions")} pe
  ON rr.policy_version_id = pe.policy_version_id
JOIN {self._table("policy_sql_versions")} psv
  ON rr.sql_version_id = psv.sql_version_id AND rr.policy_version_id = psv.policy_version_id
WHERE rr.run_id = @run_id
LIMIT 1
"""
        params = [bigquery.ScalarQueryParameter("run_id", "STRING", run_id)]
        rows = list(self.client.query(
            query, job_config=bigquery.QueryJobConfig(query_parameters=params)
        ).result())
        if not rows:
            return None
        row = rows[0]
        return {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in dict(row).items()}

    def get_sql_versions_for_policy(self, policy_id: str) -> list[dict[str, Any]]:
        query = f"""
SELECT psv.sql_version_id, psv.policy_version_id, psv.generated_sql,
       psv.validation_status, psv.approved_at
FROM {self._table("policy_sql_versions")} psv
JOIN {self._table("policy_documents")} pd
  ON psv.policy_version_id = pd.policy_version_id
WHERE pd.policy_id = @policy_id
ORDER BY psv.approved_at DESC NULLS LAST
"""
        params = [bigquery.ScalarQueryParameter("policy_id", "STRING", policy_id)]
        return [dict(row) for row in self.client.query(
            query, job_config=bigquery.QueryJobConfig(query_parameters=params)
        ).result()]

    def get_extraction_details(self, policy_version_id: str) -> dict[str, Any] | None:
        query = f"""
SELECT
  pe.extraction_json,
  pe.clause_citations,
  pe.model_version,
  psv.sql_version_id,
  psv.generated_sql,
  psv.agent_trace
FROM {self._table("policy_extractions")} pe
LEFT JOIN {self._table("policy_sql_versions")} psv
  ON pe.policy_version_id = psv.policy_version_id
WHERE pe.policy_version_id = @policy_version_id
LIMIT 1
"""
        params = [bigquery.ScalarQueryParameter("policy_version_id", "STRING", policy_version_id)]
        rows = list(self.client.query(
            query, job_config=bigquery.QueryJobConfig(query_parameters=params)
        ).result())
        if not rows:
            return None
        row = dict(rows[0])
        for key in ("extraction_json", "clause_citations", "agent_trace"):
            if row.get(key) is not None and not isinstance(row[key], (dict, list)):
                try:
                    row[key] = json.loads(str(row[key]))
                except (json.JSONDecodeError, TypeError):
                    pass
        return row

