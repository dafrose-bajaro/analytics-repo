from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import computed_field
from pydantic_settings import BaseSettings


# read environment variables
class Settings(BaseSettings):
    # configuration fields
    PYTHON_ENV: Literal["development", "production"] = "production"
    BASE_DIR: Path = Path(__file__).parent.parent
    DEFAULT_TZ: str = "Asia/Manila"

    GCP_PROJECT: str
    GCP_BUCKET: str
    GCP_ENDPOINT: str
    GCP_REGION: str

    NASA_FIRMS_MAP_KEY: str
    NASA_FIRMS_BASE_URL: str = "https://firms.modaps.eosdis.nasa.gov"

    # tell if env is in production
    @computed_field
    @property
    def IS_PRODUCTION(self) -> bool:
        return self.PYTHON_ENV == "production"

    # path to DuckDB file
    @computed_field
    @property
    def DUCKDB_DATABASE(self) -> str:
        return str(self.BASE_DIR / "data/lake/database.duckdb")


# settings instance: load once and reuse
@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
