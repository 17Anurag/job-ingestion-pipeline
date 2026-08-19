"""
Data models.

JobListing is intentionally strict: required fields are the ones we consider
load-bearing for the product. If a source's markup/response shape changes
overnight and one of these goes missing, we want a loud, structured failure
(a caught ValidationError we can log and quarantine) instead of a job with
silently blank fields flowing into the database.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class JobListing(BaseModel):
    source: str
    source_id: str  # id/guid from the source, used for de-duplication
    title: str
    company: str
    location: Optional[str] = "Not specified"
    url: str
    posted_at: Optional[datetime] = None
    tags: list[str] = Field(default_factory=list)
    fetched_at: datetime = Field(default_factory=datetime.utcnow)

    @field_validator("title", "company", "url")
    @classmethod
    def not_blank(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("required field was blank — treat as schema drift")
        return v.strip()

    def dedupe_key(self) -> str:
        return f"{self.source}:{self.source_id}"


class SourceHealth(BaseModel):
    """Live state of one adapter, used by the /status dashboard."""

    name: str
    state: str  # "closed" (healthy), "open" (tripped), "half_open" (probing)
    consecutive_failures: int = 0
    last_success_at: Optional[datetime] = None
    last_attempt_at: Optional[datetime] = None
    last_error: Optional[str] = None
    jobs_last_run: int = 0
    total_jobs_ingested: int = 0
    quarantined_items: int = 0
