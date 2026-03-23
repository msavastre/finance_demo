import json
from datetime import datetime
from typing import Any

import vertexai
from vertexai.generative_models import GenerativeModel, Part

from rwa_demo.config import settings


class PolicyAgentService:
    def __init__(self) -> None:
        if settings.use_vertex_agent:
            vertexai.init(project=settings.project_id, location=settings.location)
        self.model = GenerativeModel(settings.vertex_model)

    def generate_sql_from_policy(
        self,
        policy_gcs_uri: str,
        policy_version_id: str,
        schema_snapshot: dict[str, Any],
    ) -> tuple[str, str, dict[str, Any]]:
        """
        Non-deterministic SQL generation grounded on:
        1) policy PDF in GCS, 2) live BigQuery schema snapshot.
        """
        schema_text = json.dumps(schema_snapshot, indent=2)
        prompt = f"""
You are a Vertex AI Agent Engine SQL generator for regulatory reporting.
Task:
- Read policy rules from the provided PDF.
- Use the provided BigQuery schema to produce executable BigQuery SQL.
- SQL must calculate aggregated RWA outputs for Global Finance/Treasury.

Constraints:
- Use ONLY tables and columns present in schema snapshot.
- Output SQL that writes to `{{project}}.{{dataset}}.rwa_report_outputs`.
- Include placeholders: {{run_id}}, {{policy_id}}, {{sql_version_id}}.
- Hardcode policy_version_id as '{policy_version_id}'.
- Do not output explanations outside JSON.

Return strict JSON with keys:
- summary: short explanation of interpreted rules
- sql: generated BigQuery SQL
- clause_citations: list of key policy clauses interpreted

BigQuery schema snapshot:
{schema_text}
"""

        pdf_part = Part.from_uri(policy_gcs_uri, mime_type="application/pdf")
        result = self.model.generate_content(
            [pdf_part, prompt],
            generation_config={"temperature": 0.35, "response_mime_type": "application/json"},
        )

        parsed = json.loads(result.text)
        summary = parsed.get(
            "summary",
            f"Policy {policy_version_id} parsed at {datetime.utcnow().isoformat()}Z.",
        )
        sql = parsed["sql"].strip()
        trace = {
            "policy_gcs_uri": policy_gcs_uri,
            "model": settings.vertex_model,
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "clause_citations": parsed.get("clause_citations", []),
            "summary": summary,
        }
        return summary, sql, trace

