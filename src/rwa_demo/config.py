from dataclasses import dataclass
import os

from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class Settings:
    project_id: str = os.getenv("GOOGLE_CLOUD_PROJECT", "")
    location: str = os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1")
    dataset: str = os.getenv("BIGQUERY_DATASET", "finance_demo")
    policy_bucket: str = os.getenv("GCS_POLICY_BUCKET", "")
    use_vertex_agent: bool = os.getenv("USE_VERTEX_AGENT", "true").lower() == "true"
    demo_mode: str = os.getenv("DEMO_MODE", "cloud")
    vertex_model: str = os.getenv("VERTEX_MODEL", "gemini-3.1-pro")


settings = Settings()

