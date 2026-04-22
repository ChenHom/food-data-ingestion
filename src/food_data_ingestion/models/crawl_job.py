from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

CRAWL_JOB_STATUSES = {
    "pending",
    "running",
    "success",
    "failed",
    "partial",
    "skipped",
    "cancelled",
}

CrawlJobStatus = Literal["pending", "running", "success", "failed", "partial", "skipped", "cancelled"]


@dataclass(frozen=True)
class CrawlJobCreate:
    platform: str
    job_type: str
    source_target_id: int | None = None
    status: CrawlJobStatus = "pending"
    scheduled_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    attempt_count: int = 0
    worker_name: str | None = None
    request_meta: dict[str, Any] = field(default_factory=dict)
    stats: dict[str, Any] = field(default_factory=dict)
    error_message: str | None = None

    def __post_init__(self) -> None:
        if self.status not in CRAWL_JOB_STATUSES:
            raise ValueError(f"invalid crawl job status: {self.status}")
