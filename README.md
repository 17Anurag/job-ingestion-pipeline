# Job Ingestion Pipeline

A resilient job-listing ingestion pipeline built for a take-home
assessment. Pulls from RemoteOK's public API and WeWorkRemotely's public
RSS feed through a shared adapter interface, with per-source circuit
breakers, retry classification, schema-drift quarantine, and a live status
dashboard.

See [`DESIGN.md`](./DESIGN.md) for the full design write-up (detection
surface, ingestion strategy, resilience, and where the line is drawn) and
[`DECISIONS.md`](./DECISIONS.md) for the required 1-pager.

## Run locally

```bash
python3 -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

Notes:

- **Preferred:** run the app as a module so package imports behave correctly:

```bash
python -m app.main
```

- **Alternative:** you can run the script directly; a small compatibility
  shim in `app/main.py` makes this work:

```bash
python app/main.py
```

Then open `http://localhost:8000` for the dashboard, or:

- `POST /trigger` — run the pipeline immediately (don't wait for the
  10-minute background cadence)
- `GET /status` — JSON view of per-source circuit-breaker state
- `GET /jobs` — ingested listings (`?source=remoteok` to filter)

## Run the offline test

Exercises dedupe, schema-drift quarantine, and circuit-breaker behavior
against mocked responses (no network needed):

```bash
python3 test_pipeline.py
```

## Deploy (Render, free tier)

1. Push this repo to GitHub.
2. On [render.com](https://render.com): New → Web Service → connect the
   repo. Render will pick up `render.yaml` automatically (build:
   `pip install -r requirements.txt`, start:
   `uvicorn app.main:app --host 0.0.0.0 --port $PORT`).
3. Deploy. First load may take ~30s to spin up on the free tier.
4. Hit `POST /trigger` once after it's live so the dashboard isn't empty
   while waiting for the first scheduled run.

(Railway works the same way — set the same build/start commands manually
if not using `render.yaml`.)

## Project layout

```
app/
  adapters/
    base.py            # SourceAdapter interface every source implements
    remoteok.py         # primary source — public JSON API
    weworkremotely.py   # fallback source — public RSS feed
  models.py              # JobListing (Pydantic — this is the schema-drift gate)
  storage.py             # SQLite: jobs, quarantine, per-source circuit state
  pipeline.py            # retry/backoff, circuit breaker, orchestration
  main.py                # FastAPI app, dashboard, background scheduler
  templates/dashboard.html
test_pipeline.py         # offline test against mocked adapter responses
DESIGN.md                 # detection surface / ingestion / resilience / ethics
DECISIONS.md               # required 1-pager
```
