"""
Orchestrator. This is the part of the codebase that answers the brief's
actual question: "does this survive a source detecting/blocking it
mid-run?"

Four mechanisms, each mapped to a specific failure mode:

  1. Rate limiting + jitter   -> don't be the thing that trips detection
  2. Retry with backoff       -> transient failures (timeouts, 5xx) don't
                                  count as "the source is blocking us"
  3. Circuit breaker          -> sustained failures (repeated 403/429/empty)
                                  stop hammering a source that's actively
                                  blocking, and record *why* it tripped
  4. Fallback ordering        -> if the primary source's breaker is open,
                                  the run still produces jobs from the
                                  secondary source instead of an empty result
"""
from __future__ import annotations

import asyncio
import random
from datetime import datetime, timedelta
from typing import Optional

import httpx
from pydantic import ValidationError

from app import storage
from app.adapters.base import SourceAdapter

FAILURE_THRESHOLD = 3          # consecutive failures before breaker opens
OPEN_COOLDOWN = timedelta(minutes=5)   # how long breaker stays open before probing again
MIN_REQUEST_GAP = 1.5          # seconds, baseline pacing between requests to a source
JITTER = 1.0                   # +/- random seconds added to pacing (avoid metronomic timing)
MAX_RETRIES = 3


class CircuitOpenError(Exception):
    pass


async def _polite_delay() -> None:
    """Jittered pacing so requests aren't spaced at suspiciously regular intervals."""
    await asyncio.sleep(MIN_REQUEST_GAP + random.uniform(-JITTER / 2, JITTER))


def _breaker_allows_attempt(name: str) -> bool:
    state = storage.get_source_state(name)
    if state is None or state["state"] == "closed":
        return True
    if state["state"] == "open":
        opened_at = state.get("opened_at")
        if opened_at and datetime.utcnow() - datetime.fromisoformat(opened_at) > OPEN_COOLDOWN:
            # cooldown elapsed -> allow one probe request (half-open)
            storage.upsert_source_state(name=name, state="half_open")
            return True
        return False
    return True  # half_open: allow the probe through


def _record_success(name: str, jobs_count: int) -> None:
    now = datetime.utcnow().isoformat()
    storage.upsert_source_state(
        name=name, state="closed", consecutive_failures=0,
        last_success_at=now, last_attempt_at=now, last_error=None,
        jobs_last_run=jobs_count, opened_at=None,
    )


def _record_failure(name: str, error: str) -> None:
    state = storage.get_source_state(name) or {}
    failures = (state.get("consecutive_failures") or 0) + 1
    now = datetime.utcnow().isoformat()
    new_state = "open" if failures >= FAILURE_THRESHOLD else state.get("state", "closed")
    storage.upsert_source_state(
        name=name, state=new_state, consecutive_failures=failures,
        last_success_at=state.get("last_success_at"), last_attempt_at=now,
        last_error=error, jobs_last_run=0,
        opened_at=now if new_state == "open" and state.get("state") != "open" else state.get("opened_at"),
    )


async def _fetch_with_retry(adapter: SourceAdapter, client: httpx.AsyncClient):
    last_exc: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            await _polite_delay()
            return await adapter.fetch(client)
        except httpx.HTTPStatusError as e:
            last_exc = e
            # 429/403 are signals to back off hard, not just retry fast —
            # this is the "source is actively rate-limiting/blocking us"
            # branch, distinct from a transient 500 or timeout.
            if e.response.status_code in (403, 429):
                backoff = (2 ** attempt) + random.uniform(0, 1)
                await asyncio.sleep(backoff)
            else:
                await asyncio.sleep(1)
        except (httpx.TimeoutException, httpx.TransportError) as e:
            last_exc = e
            await asyncio.sleep(2 ** attempt)
    raise last_exc if last_exc else RuntimeError("fetch failed with no exception captured")


async def run_source(adapter: SourceAdapter, client: httpx.AsyncClient) -> dict:
    """
    Runs one adapter end to end. Never raises on expected failure modes —
    always returns a result dict so the caller (run_pipeline) can move on
    to the next source instead of the whole run dying.
    """
    result = {"source": adapter.name, "new_jobs": 0, "seen_jobs": 0, "quarantined": 0, "status": "ok"}

    if not _breaker_allows_attempt(adapter.name):
        result["status"] = "circuit_open_skipped"
        return result

    try:
        raw = await _fetch_with_retry(adapter, client)
    except Exception as e:
        _record_failure(adapter.name, str(e))
        result["status"] = f"fetch_failed: {e}"
        return result

    records = list(adapter.to_records(raw))

    if not records:
        # Empty response is treated as a *signal*, not silently ignored —
        # zero jobs from a source that normally has hundreds is far more
        # likely to mean "we got an empty anti-bot response" than "no jobs
        # exist today."
        _record_failure(adapter.name, "empty response (possible soft-block or markup change)")
        result["status"] = "empty_response_flagged"
        return result

    new_count = 0
    for record in records:
        try:
            job = adapter.parse(record)
        except ValidationError as e:
            storage.quarantine_item(adapter.name, record, str(e))
            result["quarantined"] += 1
            continue
        is_new = storage.upsert_job(job)
        if is_new:
            new_count += 1

    result["new_jobs"] = new_count
    result["seen_jobs"] = len(records)
    _record_success(adapter.name, len(records))
    return result


async def run_pipeline(adapters: list[SourceAdapter]) -> list[dict]:
    """
    Runs every configured source, in order. Sources aren't mutually
    exclusive fallbacks at the API layer (each is independently useful
    data) but the ordering matters: if the primary is breaker-open, the
    run still returns a non-empty result from whichever source downstream
    is still healthy, which is the behavior the brief asks for under
    "fallback when a source starts blocking you mid-run."
    """
    results = []
    async with httpx.AsyncClient(follow_redirects=True) as client:
        for adapter in adapters:
            res = await run_source(adapter, client)
            results.append(res)
    return results
