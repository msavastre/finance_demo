from rwa_demo.bq_repository import BigQueryRepository
from rwa_demo.config import settings


class SqlExecutionAgent:
    """
    Executes approved SQL and stores output in versioned table.
    """

    def __init__(self, repo: BigQueryRepository) -> None:
        self.repo = repo

    def run(
        self,
        sql_template: str,
        run_id: str,
        policy_id: str,
        policy_version_id: str,
        sql_version_id: str,
    ) -> None:
        sql = (
            sql_template.replace("{project}", settings.project_id)
            .replace("{dataset}", settings.dataset)
            .replace("{run_id}", run_id)
            .replace("{policy_id}", policy_id)
            .replace("{sql_version_id}", sql_version_id)
        )

        self.repo.execute_sql(sql)

