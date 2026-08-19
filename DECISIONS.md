# DECISIONS.md

## 1. Why this ingestion strategy over the obvious alternative I rejected?

The obvious alternative is a headless browser (Playwright/Puppeteer) doing
full page renders with stealth patches, since that's what most "job
scraper" tutorials reach for. I rejected it for this scope because it
solves a problem I don't have yet: RemoteOK and WeWorkRemotely are public,
scrape-friendly API/feed endpoints, so paying the cost of a browser
(slower, heavier to deploy on a free host, and its own fingerprint surface
to maintain) buys nothing here. Instead I put the effort into the part
that generalizes regardless of source type — a circuit breaker, retry
classification, and schema-drift quarantine — because that's what
actually determines whether the pipeline survives a source turning
hostile, and it's reusable infrastructure a headless-browser adapter would
sit behind just as easily as the two HTTP adapters do now.

## 2. One trade-off I made under the time limit, and what I'd do with a
real week

I did not build proxy/IP rotation — the identity abstraction
(`adapters/base.py`'s header set, kept as a unit rather than mixed-and-
matched) is there, but it's currently one identity per source, not a
rotating pool. With a real week I'd add a small proxy-pool client behind
that same seam, plus promote the in-memory/SQLite circuit-breaker state to
something shared across multiple worker processes (right now a
multi-process deploy would have each process tracking its own breaker
state, which defeats the point once you scale past one instance).

## 3. Where I used AI tools, and what I verified/changed afterward

I used Claude to scaffold the initial adapter/pipeline structure and the
dashboard HTML/CSS. What I personally verified and changed:

- Wrote and ran `test_pipeline.py` against mocked responses to confirm the
  three failure modes actually behave as designed — dedupe on a
  second identical run, quarantine on a record with a blank required
  field, and the circuit breaker opening after 3 consecutive 403s while
  the *other* source keeps ingesting. All assertions pass; output is
  reproducible by running the script.
- Checked the retry logic's status-code branching (403/429 treated as
  "back off hard" vs. timeout/5xx treated as "retry short") against how
  real rate limiters typically respond, and adjusted the backoff formula.
- Removed an initial draft that used a shared `except Exception` across
  fetch/parse/store — rewrote it so schema-drift, rate-limit, and
  transient-network failures are distinguished, since collapsing them
  would have made the "resilience" claim in the design doc false.
- Verified the dependency versions actually boot together (an unpinned
  install initially hit a Jinja2/Starlette template-cache
  incompatibility) and pinned exact versions in `requirements.txt` so the
  deployed instance doesn't hit the same issue.

I can walk through and defend every line of `pipeline.py` and the storage
layer in the follow-up call — that's the part I spent the most time
actually reasoning about rather than accepting as generated.
