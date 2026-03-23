from google.cloud import storage

from rwa_demo.config import settings


class PolicyStorage:
    def __init__(self) -> None:
        self.client = storage.Client(project=settings.project_id)
        self.bucket = self.client.bucket(settings.policy_bucket)

    def upload_policy_pdf(self, policy_version_id: str, file_bytes: bytes, filename: str) -> str:
        path = f"policies/{policy_version_id}/{filename}"
        blob = self.bucket.blob(path)
        blob.upload_from_string(file_bytes, content_type="application/pdf")
        return f"gs://{settings.policy_bucket}/{path}"

