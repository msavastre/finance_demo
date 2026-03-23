import json
import uuid

from rwa_demo.agent_service import PolicyAgentService
from rwa_demo.bq_repository import BigQueryRepository
from rwa_demo.sql_executor import SqlExecutionAgent
from rwa_demo.storage import PolicyStorage


class DemoWorkflowService:
    def __init__(self) -> None:
        self.repo = BigQueryRepository()
        self.storage = PolicyStorage()
        self.agent = PolicyAgentService()
        self.exec_agent = SqlExecutionAgent(self.repo)

    def upload_policy(
        self,
        uploaded_by: str,
        filename: str,
        file_bytes: bytes,
        existing_policy_id: str | None = None,
        supersedes_policy_version_id: str | None = None,
    ) -> tuple[str, str, str]:
        policy_id = existing_policy_id or f"POL-{uuid.uuid4().hex[:10].upper()}"
        policy_version_id = f"{policy_id}-V{uuid.uuid4().hex[:6].upper()}"
        gcs_uri = self.storage.upload_policy_pdf(policy_version_id, file_bytes, filename)
        self.repo.register_policy_document(
            policy_id=policy_id,
            policy_version_id=policy_version_id,
            uploaded_by=uploaded_by,
            gcs_uri=gcs_uri,
            supersedes_policy_version_id=supersedes_policy_version_id,
        )
        return policy_id, policy_version_id, gcs_uri

    def generate_sql(self, policy_version_id: str) -> tuple[str, str, str]:
        policy_gcs_uri = self.repo.get_policy_gcs_uri(policy_version_id)
        schema_snapshot = self.repo.get_schema_snapshot()
        summary, generated_sql, agent_trace = self.agent.generate_sql_from_policy(
            policy_gcs_uri=policy_gcs_uri,
            policy_version_id=policy_version_id,
            schema_snapshot=schema_snapshot,
        )
        sql_version_id = f"SQL-{uuid.uuid4().hex[:10].upper()}"
        self.repo.save_policy_extraction(
            policy_version_id=policy_version_id,
            extraction_summary=summary,
            extraction_json=json.dumps(
                {
                    "policy_gcs_uri": policy_gcs_uri,
                    "summary": summary,
                    "schema_snapshot": schema_snapshot,
                }
            ),
        )
        self.repo.save_generated_sql(
            sql_version_id=sql_version_id,
            policy_version_id=policy_version_id,
            generated_sql=generated_sql,
            agent_trace=agent_trace,
            schema_snapshot=schema_snapshot,
        )
        return sql_version_id, summary, generated_sql

    def approve_sql(self, sql_version_id: str, approved_by: str) -> None:
        self.repo.approve_sql(sql_version_id=sql_version_id, approved_by=approved_by)

    def execute_sql_agent(
        self,
        policy_id: str,
        policy_version_id: str,
        sql_version_id: str,
    ) -> str:
        run_id = f"RUN-{uuid.uuid4().hex[:10].upper()}"
        self.repo.create_report_run(
            run_id=run_id,
            policy_id=policy_id,
            policy_version_id=policy_version_id,
            sql_version_id=sql_version_id,
        )
        try:
            sql = self.repo.get_latest_generated_sql(sql_version_id)
            if not sql:
                raise ValueError(f"No SQL found for {sql_version_id}")
            self.exec_agent.run(
                sql_template=sql,
                run_id=run_id,
                policy_id=policy_id,
                policy_version_id=policy_version_id,
                sql_version_id=sql_version_id,
            )
            self.repo.complete_report_run(run_id=run_id, status="succeeded")
        except Exception:
            self.repo.complete_report_run(run_id=run_id, status="failed")
            raise

        return run_id

