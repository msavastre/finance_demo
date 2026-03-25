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
- sql: generated BigQuery SQL (include inline comments like '-- [C1] ...' referencing clause IDs)
- clause_citations: list of objects, each with:
    - clause_id: string like "C1", "C2", etc.
    - clause_text: the exact policy text or close paraphrase
    - clause_type: one of "threshold", "mapping", "calculation", "exclusion", "definition"
    - sql_section: brief description of which SQL section implements this clause

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

    def explain_policy_delta(self, original_gcs_uri: str, updated_gcs_uri: str) -> str:
        """Tell Gemini to compare two PDFs and explain what rules changed."""
        prompt = """
You are a senior regulatory capital analyst.
Compare the rules in these two versions of the policy document (Original vs Updated).
Summarize what changed between them and what it means for RWA calculations.
Highlight structural shifts in thresholds, mappings, or definitions.
Concisely format your response in markdown. Use standard banking/Basel terminology.
"""
        orig_part = Part.from_uri(original_gcs_uri, mime_type="application/pdf")
        upd_part = Part.from_uri(updated_gcs_uri, mime_type="application/pdf")
        
        result = self.model.generate_content(
            [orig_part, upd_part, prompt],
            generation_config={"temperature": 0.2},
        )
        return result.text

    def regenerate_sql_with_feedback(
        self,
        policy_gcs_uri: str,
        policy_version_id: str,
        schema_snapshot: dict[str, Any],
        feedback: str,
        original_agent_trace: dict[str, Any],
    ) -> tuple[str, str, dict[str, Any]]:
        """
        Regenerate SQL by appending user feedback to the standard prompt.
        """
        schema_text = json.dumps(schema_snapshot, indent=2)
        base_prompt = f"""
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
- clause_citations: list of objects [...]

BigQuery schema snapshot:
{schema_text}

=== USER FEEDBACK ===
The user was not satisfied with the previous generation. Please apply this manual override/correction:
"{feedback}"
"""
        pdf_part = Part.from_uri(policy_gcs_uri, mime_type="application/pdf")
        result = self.model.generate_content(
            [pdf_part, base_prompt],
            generation_config={"temperature": 0.2, "response_mime_type": "application/json"},
        )

        parsed = json.loads(result.text)
        summary = parsed.get("summary", f"Regenerated SQL based on user feedback.")
        sql = parsed["sql"].strip()
        trace = {
            "policy_gcs_uri": policy_gcs_uri,
            "model": settings.vertex_model,
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "clause_citations": parsed.get("clause_citations", []),
            "summary": summary,
            "user_feedback": feedback,
        }
        return summary, sql, trace

    def answer_rwa_question(self, question: str, context: dict[str, Any]) -> str:
        """Answer a natural language question about RWA data using Gemini."""
        context_text = json.dumps(context, indent=2, default=str)
        prompt = f"""You are a regulatory capital analyst for Global Finance and Treasury.

Context — RWA reporting data:
{context_text}

Question: {question}

Answer concisely and accurately for a senior banking audience. Reference specific dollar amounts
and percentages where relevant. Use standard banking/Basel III terminology."""
        result = self.model.generate_content(
            prompt,
            generation_config={"temperature": 0.2},
        )
        return result.text

