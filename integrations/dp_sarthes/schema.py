from typing import TypedDict

from api.dia_log_client.models import PeriodRecurrenceTypeEnum


class SarthesMeasure(TypedDict):
    id: str
    title: str
    max_speed: int
    geometry: str
    label: str
    # Period fields (prefixed with period_)
    period_start_date: str | None
    period_end_date: str | None
    period_start_time: str | None
    period_end_time: str | None
    period_recurrence_type: PeriodRecurrenceTypeEnum | None
    period_is_permanent: bool | None