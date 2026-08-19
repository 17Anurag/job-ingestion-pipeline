"""
Offline test: mocks the two adapters' fetch() so we can verify parsing,
dedupe, quarantine, and circuit-breaker logic without hitting the real
network (this sandbox has no route to remoteok.com / weworkremotely.com,
and this also lets us deterministically simulate a source failing).
"""
import asyncio
import sys
sys.path.insert(0, ".")

from app import storage
from app.adapters.remoteok import RemoteOKAdapter
from app.adapters.weworkremotely import WeWorkRemotelyAdapter
from app.pipeline import run_source, _breaker_allows_attempt

FAKE_REMOTEOK_RESPONSE = [
    {"legal": "some legend object remoteok puts first"},
    {"id": "1001", "position": "Backend Engineer", "company": "Acme Corp",
     "location": "Remote", "url": "https://remoteok.com/jobs/1001", "epoch": 1720000000, "tags": ["python", "backend"]},
    {"id": "1002", "position": "", "company": "Broken Co",  # missing title -> should quarantine
     "location": "Remote", "url": "https://remoteok.com/jobs/1002", "epoch": 1720000000, "tags": []},
    {"id": "1003", "position": "Frontend Engineer", "company": "Widgets Inc",
     "location": "Remote", "url": "https://remoteok.com/jobs/1003", "epoch": 1720000000, "tags": ["react"]},
]

FAKE_WWR_RSS = """<?xml version="1.0"?>
<rss><channel>
<item><title>DataCo: Senior Data Engineer</title><link>https://weworkremotely.com/jobs/2001</link>
<pubDate>Mon, 01 Jul 2024 10:00:00 +0000</pubDate><guid>2001</guid></item>
<item><title>ShipFast: Platform Engineer</title><link>https://weworkremotely.com/jobs/2002</link>
<pubDate>Mon, 01 Jul 2024 11:00:00 +0000</pubDate><guid>2002</guid></item>
</channel></rss>"""


class FakeClient:
    """Stand-in for httpx.AsyncClient — good/failing modes controlled by test."""
    def __init__(self, mode="ok"):
        self.mode = mode

    async def get(self, url, headers=None, timeout=None):
        import httpx
        if self.mode == "block":
            request = httpx.Request("GET", url)
            resp = httpx.Response(403, request=request, text="blocked")
            raise httpx.HTTPStatusError("blocked", request=request, response=resp)
        if "remoteok" in url:
            return FakeResponse(FAKE_REMOTEOK_RESPONSE, is_json=True)
        return FakeResponse(FAKE_WWR_RSS, is_json=False)


class FakeResponse:
    def __init__(self, data, is_json):
        self._data = data
        self._is_json = is_json
        self.status_code = 200
    def raise_for_status(self):
        pass
    def json(self):
        return self._data
    @property
    def text(self):
        return self._data


async def main():
    # Speed up pacing/backoff for the test run only — production values are
    # unchanged in app/pipeline.py.
    import app.pipeline as pipeline_mod
    pipeline_mod.MIN_REQUEST_GAP = 0.05
    pipeline_mod.JITTER = 0.02
    _real_sleep = asyncio.sleep
    async def _fast_sleep(seconds):
        await _real_sleep(min(seconds, 0.05))
    pipeline_mod.asyncio.sleep = _fast_sleep

    storage.DB_PATH.unlink(missing_ok=True)
    storage.init_db()

    print("--- Run 1: RemoteOK healthy ---")
    client = FakeClient(mode="ok")
    res = await run_source(RemoteOKAdapter(), client)
    print(res)
    assert res["new_jobs"] == 2, "expected 2 valid jobs (1 quarantined for blank title)"
    assert res["quarantined"] == 1
    assert storage.count_jobs("remoteok") == 2
    assert storage.count_quarantine("remoteok") == 1
    print("Jobs table:", storage.count_jobs(), "| Quarantine table:", storage.count_quarantine())

    print("\n--- Run 2: same data again (dedupe check) ---")
    res2 = await run_source(RemoteOKAdapter(), client)
    print(res2)
    assert res2["new_jobs"] == 0, "re-running identical data should not create duplicates"
    assert storage.count_jobs("remoteok") == 2

    print("\n--- Run 3: WeWorkRemotely (structurally different source) ---")
    res3 = await run_source(WeWorkRemotelyAdapter(), client)
    print(res3)
    assert res3["new_jobs"] == 2
    assert storage.count_jobs() == 4

    print("\n--- Run 4-6: RemoteOK gets blocked (403) repeatedly -> circuit should open ---")
    blocked_client = FakeClient(mode="block")
    for i in range(3):
        r = await run_source(RemoteOKAdapter(), blocked_client)
        print(f"  attempt {i+1}: {r['status']}")
    state = storage.get_source_state("remoteok")
    print("Breaker state after 3 failures:", state["state"], "| consecutive_failures:", state["consecutive_failures"])
    assert state["state"] == "open"
    assert _breaker_allows_attempt("remoteok") is False, "breaker should now be rejecting attempts"

    print("\n--- Run 7: pipeline still returns WWR jobs even though RemoteOK breaker is open ---")
    r7 = await run_source(WeWorkRemotelyAdapter(), client)
    print(r7)
    assert r7["status"] == "ok"

    print("\nALL ASSERTIONS PASSED")

asyncio.run(main())
