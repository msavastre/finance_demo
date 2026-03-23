from datetime import datetime, date
from pydantic import BaseModel, Field


class PolicyDocument(BaseModel):
    policy_id: str
    policy_version_id: str
    uploaded_by: str
    gcs_uri: str
    uploaded_at: datetime = Field(default_factory=datetime.utcnow)
    supersedes_policy_version_id: str | None = None


class GeneratedSql(BaseModel):
    sql_version_id: str
    policy_version_id: str
    generated_sql: str
    validation_status: str = "pending"
    approved_by: str | None = None
    approved_at: datetime | None = None


class ReportRun(BaseModel):
    run_id: str
    policy_id: str
    policy_version_id: str
    sql_version_id: str
    run_status: str
    started_at: datetime = Field(default_factory=datetime.utcnow)
    ended_at: datetime | None = None


class ReportOutputRow(BaseModel):
    portfolio: str
    risk_bucket: str
    rwa_amount: float
    as_of_date: date
    run_id: str
    policy_id: str
    policy_version_id: str
    sql_version_id: str

