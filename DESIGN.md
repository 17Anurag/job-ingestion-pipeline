# Design Document — Job Ingestion Pipeline

## 0. Scope note

The live demo runs against RemoteOK's public JSON API and WeWorkRemotely's
public RSS feed — both explicitly published for programmatic consumption,
per the task's guardrail against touching a real LinkedIn-style account.
Neither has an anti-bot layer to defeat. So sections 1–2 below describe the
detection surface and countermeasures **the architecture is built to
handle** (and would need, unmodified in shape, to point at a harder
source like Indeed or Naukri) — not surface I claim to have defeated on a
live target. Section 3 (resilience) and 4 (the line) are demonstrated as-is
in the running code.

## 1. Detection surface

What actually gives an automated client away, roughly in the order a real
anti-bot stack checks them:

- **Network/TLS fingerprint** — `requests`/`httpx`'s TLS ClientHello (JA3
  hash) and HTTP/2 frame ordering don't match a real Chrome or Firefox.
  This is checked before a single byte of your request line is read.
- **Headless browser fingerprints** — `navigator.webdriver === true`,
  missing/mocked `navigator.plugins`, no WebGL renderer string or a
  software one (SwiftShader), inconsistent screen/viewport ratios,
  automation-only JS globals (`window.cdc_...` from ChromeDriver).
- **HTTP header shape** — missing `Accept-Language`, `sec-ch-ua*` client
  hints, or a `Referer` chain that doesn't tell a coherent story (e.g. a
  request to a job page with no prior request to the listing page).
- **Request timing** — machine-regular intervals (exactly every 2.000s),
  or response-to-next-request latency too fast for a page to have
  rendered and a human to have clicked.
- **Behavioral absence** — no mouse movement/scroll events before a form
  submit or "load more" click, instant multi-page traversal, zero
  variance in dwell time per page.
- **Volume/velocity per identity** — one IP or one session pulling far
  more pages than a human browsing session would in that time window.

What this design accounts for: header shape, timing/behavioral variance,
and per-identity volume (§2). What it does **not** attempt: TLS
fingerprint spoofing or full headless-browser stealth patching — those are
an arms race against the specific target's detection vendor, need
per-target tuning, and are exactly where I'd stop building without a
legal/ToS sign-off (§4).

## 2. Ingestion strategy

- **Rotation** — each "identity" (persona) is defined as a pinned
  {user-agent, Accept-Language, header order} tuple plus its own cookie
  jar, rotated as a unit — not independently, because a Chrome 124 UA
  paired with Firefox's header order is itself a fingerprint mismatch.
  For a harder target, identities would sit behind a rotating residential
  proxy pool (datacenter IP ranges are the first thing rate-limiters
  block); the current code has one identity per source, which is the
  seam where a proxy pool plugs in.
- **Pacing** — token-bucket pacing with randomized jitter between
  requests (`pipeline.py::_polite_delay`), so intervals aren't
  metronomic. For a real target this would also include randomized
  "browsing" requests (category pages, not just target pages) to make
  the session look like a person, not a scraper hitting only endpoints
  it wants.
- **Session/identity management** — cookies persist per identity across
  requests within a run rather than a fresh anonymous request each time,
  which is closer to how a returning visitor behaves.
- **Fallback when a source gets blocked mid-run** — this is the circuit
  breaker (`pipeline.py`): after 3 consecutive failures a source trips to
  `open` and stops being hit for a 5-minute cooldown, then makes one
  `half_open` probe request before resuming. Meanwhile `run_pipeline`
  still executes every *other* configured adapter, so a run against a
  blocked source degrades to partial data instead of a total failure —
  demonstrated live in `/status` and the dashboard.
- **Plan B if the primary approach gets shut down in a week** — for a
  source that starts hard-blocking every identity in the rotation, the
  fallback isn't "try harder," it's "add another source." The adapter
  interface (`adapters/base.py`) is the seam: a new source is a new class
  implementing `fetch`/`to_records`/`parse`, registered in `main.py`'s
  `ADAPTERS` list. RemoteOK → WeWorkRemotely in this repo is that pattern
  exercised for real.

## 3. Resilience

Three failure modes, each handled distinctly rather than caught by one
generic `except Exception`:

- **Markup/schema changes overnight** — every parsed record goes through
  `JobListing` (Pydantic). A field that used to be present and now isn't
  raises `ValidationError`, and that record is quarantined
  (`storage.quarantine_item`) with the raw payload and error, not
  silently dropped or inserted with a blank field. The pipeline keeps
  running on every other record in that batch. Quarantined counts are
  visible on `/status` and the dashboard — this is a real, load-bearing
  concept in the running code, not a comment about it: see the
  `Broken Co` / blank-title record deliberately mixed into
  `test_pipeline.py`'s fixture, which the test asserts gets quarantined
  while its two well-formed neighbors still ingest.
- **Rate-limited or blocked mid-run** — `_fetch_with_retry` distinguishes
  a 403/429 (back off hard, exponential + jitter) from a timeout/5xx
  (retry a couple times, short backoff) from repeated failure (trip the
  circuit breaker, §2). These aren't the same failure and treating them
  identically is how naive scrapers either get permanently banned
  (retrying into a block) or give up on transient blips.
- **Empty response** — treated as a signal, not a no-op. A source that
  normally returns hundreds of jobs returning zero is far more likely to
  mean "soft-blocked" or "markup changed under us" than "no jobs exist
  today," so it's routed through the same failure-counting path as an
  explicit error (`pipeline.py::run_source`, the `if not records:`
  branch).

## 4. Where I'd stop

Personal/technical line: I will not build a scraper that logs into a real
user account to pull data (session hijacking risk, direct ToS violation
with an identifiable account attached), and I will not add a
CAPTCHA-solving or TLS-spoofing layer aimed at a specific commercial
platform's anti-bot vendor — that's a targeted defeat of a system someone
built specifically to be defeated, not "getting data out of the web," and
it's the point where I'd want legal sign-off before writing another line
of code, take-home or not.

Technically, the architecture respects that line by design, not by
promise: there's no login flow anywhere in this codebase, no fingerprint
spoofing beyond ordinary polite-client headers, and the adapters only
target sources that publish a public feed for programmatic use. Extending
this to a harder source is additive (new adapter, proxy pool behind the
identity abstraction) — it doesn't require ripping out a "respect the
target" assumption baked in elsewhere, because there isn't one to rip out.
