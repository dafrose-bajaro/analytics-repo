from datetime import datetime
from zoneinfo import ZoneInfo

from dagster import DailyPartitionsDefinition

from src.settings import settings

daily_partitions_def = DailyPartitionsDefinition(
    start_date=datetime(2024, 10, 1, tzinfo=ZoneInfo(settings.DEFAULT_TZ)),
    hour_offset=12,
    timezone=settings.DEFAULT_TZ,
)
