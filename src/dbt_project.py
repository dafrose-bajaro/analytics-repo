from dagster_dbt import DbtProject

from src.settings import settings

dbt_project = DbtProject(project_dir=settings.BASE_DIR)

dbt_project.prepare_if_dev()
