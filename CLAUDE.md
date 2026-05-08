# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common commands

Install: `pip install -r requirements.txt` (Python 3.10+; uses PEP 604 `X | None` syntax).

Run the API locally: `uvicorn app.main:app --reload --host 0.0.0.0 --port 8000`. The `0.0.0.0` host lets a physical iOS/Android device on the same Wi-Fi reach the backend.

Run a worker locally: `SERVICE_MODE=worker python start_service.py`. With no `PORT` set, this runs only the worker loop; with `PORT` set, it also exposes a tiny stdlib HTTP server on `/api/v1/health`, `/api/v1/health/live`, and `/api/v1/health/ready` so Railway healthchecks pass against worker replicas.

Tests use stdlib `unittest` (no pytest config). Run all tests: `python -m unittest discover -s tests`. Run one file: `python -m unittest tests.test_downloader`. Run one test: `python -m unittest tests.test_downloader.SomeTest.test_case`. Tests stub heavy deps (`yt_dlp`, `app.config`) at module level before import; preserve that pattern when adding tests that touch downloader/config.

Category migration script (one-shot, against the configured Supabase): `python scripts/migrate_categories.py`.

## Architecture

**Scope: Instagram only.** Reels, posts, and IGTV. Non-Instagram URLs (YouTube, TikTok, generic web) are rejected at `resolve_source_identity` with a `ValueError` that the API surfaces as HTTP 400 `invalid_request`.

This is a two-process FastAPI service: an **API** (`app/main.py`) and a **worker loop** (`app/tasks.py:run_worker`). Both are launched via `start_service.py`, switched by the `SERVICE_MODE` env var. The Procfile wires `web` to API and `worker` to worker for Railway.

**The queue is the database.** Workers poll the Supabase `processing_jobs` table directly via `claim_available_processing_jobs`, claim rows atomically, and heartbeat into a `service_health` table. There is no Redis/Dramatiq broker.

**Pipeline (`app/pipeline.py`)** runs five stages with per-step durations recorded:
1. `prepare_content_for_source` (`app/services/platform_handlers.py`) dispatches by content type into `instagram_reel` or `instagram_post`. Reels go video-only; posts can be image carousels (OCR via Groq vision) or video.
2. `transcribe_audio` (`app/services/transcriber.py`) runs Groq Whisper on downloaded audio.
3. `extract_structured_data` (`app/services/extractor.py`) calls Groq Llama to produce title, summary, content domain, topical tags, locations, key facts, people, and actionable items.
4. `embed_and_store` (`app/services/embedder.py`) writes vectors to Pinecone.
5. `save_reel` (`app/services/database.py`) persists the structured row to the Supabase `reels` table, then `assign_user_category` finalizes per-user category labels.

A processing-cache layer (`processing_cache` table) is consulted at the start of the pipeline keyed by `(source_platform, source_content_id)`. Cache hits short-circuit download/transcribe/extract; only embedding and per-user save run.

**Source identity (`app/services/source_identity.py`)** is the canonical normalizer for incoming URLs. Always route a user-submitted URL through `resolve_source_identity` before any DB lookup or pipeline call; downstream code keys on `normalized_url`, `source_platform`, `source_content_type`, and `source_content_id`. Duplicate-detection in the API uses both `find_*_by_user_and_url` and `find_*_by_user_and_source_identity` because the same reel can be shared under multiple URL shapes (`instagram.com`, `instagr.am`, with or without `igsh`/`utm_*` params).

**Download fallbacks (`app/services/downloader.py`)** for Instagram, in order: direct public page fetch, anonymous yt-dlp, Apify scraper (if `APIFY_API_TOKEN` is set), then per-cookie-slot authenticated API + public-page-with-cookies + yt-dlp-with-cookies, then `YTDLP_COOKIES_FROM_BROWSER` (local dev only). Cookie slots are loaded from `INSTAGRAM_ACTIVE_*`, `INSTAGRAM_BACKUP_*`, `INSTAGRAM_TERTIARY_*` (each accepts `_COOKIE_DATA_BASE64`, `_COOKIE_DATA`, or `_COOKIES_FILE`). The downloader records which slot succeeded for health/observability. Safe rotation: load new cookie into backup, redeploy and verify, promote backup to active, then clear backup.

**Worker concurrency** is bounded globally (`WORKER_CONCURRENCY`) and for the single supported platform (`WORKER_INSTAGRAM_CONCURRENCY`). The claim loop dedupes by `source_key` (`instagram:<shortcode>`) so two replicas don't process the same URL simultaneously. Stale-claim recovery runs every `WORKER_RECOVERY_INTERVAL_SECONDS` against jobs older than `WORKER_STALE_JOB_MINUTES`. The `WORKER_TIKTOK_CONCURRENCY`, `WORKER_YOUTUBE_CONCURRENCY`, and `WORKER_WEB_CONCURRENCY` env fields still exist in `Settings` but are no longer read.

**Failure classification (`app/services/failures.py`)** maps raised exceptions to the `FailureCode` enum (`auth_failure`, `rate_limit`, `no_audio`, etc.). `app/services/retry_policy.py` then decides whether to retry, dead-letter, or fail terminally based on the code and attempt count. The API uses the same classifier to map errors to user-facing messages and HTTP status codes via `app/services/api_responses.py`.

**Observability** is split: `app/services/observability.py` produces structured `log_processing_event` lines, `app/services/dashboard.py` and `app/services/health_checks.py` aggregate the data for the admin dashboard and `/api/v1/health/*` endpoints, and `app/services/ops_alerts.py` sends Instagram-cookie failure alerts to the admin user.

**Config (`app/config.py`)** uses `pydantic-settings` and is `lru_cache`d. Either `SUPABASE_SERVICE_ROLE_KEY` or the legacy `SUPABASE_KEY` works (`resolved_supabase_key` picks the first present). Firebase credentials accept either a path (`FIREBASE_SERVICE_ACCOUNT_PATH`) or inline JSON (`FIREBASE_SERVICE_ACCOUNT_JSON`).

## Project conventions

Tests stub external SDKs at the top of the test file (see `tests/test_downloader.py`) rather than relying on a global conftest, because importing `app.config` requires env vars; preserve this pattern.

Service-layer modules under `app/services/*.py` are the canonical place for new business logic; `app/main.py` should stay thin and route into services. Heavy lifting that is reused between the API path (`/process-reel`) and the queued path (`POST /processing-jobs/reels` then worker) lives in `app/pipeline.py` and is called by both `main.py` and `tasks.py:process_reel_job`.

If you re-introduce another platform later, extend `resolve_source_identity` (currently raises `ValueError` for non-IG hosts), add a `_prepare_<platform>` in `app/services/platform_handlers.py`, restore the platform branches in `app/services/downloader.py` (`download_media`, `_platform_key`, `_preferred_download_format`, `_platform_name`), and update `_derive_platform` and `_platform_limits` in `app/tasks.py`. Tests under `tests/test_source_identity.py`, `tests/test_platform_handlers.py`, and `tests/test_queue_control.py` currently assume Instagram-only.

## Global rules (from `~/.claude/CLAUDE.md`)

These apply to every project; the high-impact items for this repo are listed here so future sessions don't miss them.

**Git**
- Never add `Co-Authored-By: Claude` or any AI attribution in commits, code, or comments.
- Always sign commits with `-s`.
- Always `git fetch` before committing or pushing.
- Prefer `git rebase` over `git merge`. Never force-push to `main`/`master`/`develop`; use `--force-with-lease` on feature branches only when necessary.
- Always check `git status` before switching branches or rebasing.

**Code style and changes**
- Read existing code first and match its style, indentation, naming, and import ordering exactly.
- Keep changes minimal and focused. Don't refactor, rename, or "improve" surrounding code unless asked.
- Don't add docstrings, type annotations, or comments to code you didn't change. Only add comments where logic is non-obvious.
- Don't introduce new dependencies without asking.
- Don't add feature flags, backwards-compat shims, or forward-looking "just in case" code.

**Bug investigation**
- Start with evidence (full log, traceback, code at the failing line), not hypothesis. Quote errors verbatim.
- Trace from symptom to root cause; the stack trace shows where an exception was raised, not always where the bug is.
- Before changing a function, grep all call sites and check blast radius.
- State explicit tradeoffs for every fix: what it fixes, what it doesn't, what could break, what alternatives exist.
- Smallest reversible change wins. No bundling cleanup into a bug fix.
- Ship a regression test that fails pre-fix and passes post-fix; if impractical, document manual verification.
- No silent failures (`try/except: pass`, `return None`, `or ""`) without an explicit reason.
- Extra scrutiny on payment, subscription, Stripe, auth, and permission code.

**Documentation and writing**
- No em dashes anywhere. Use commas, semicolons, periods, or parentheses.
- No inflated language: avoid "crucial", "pivotal", "vital", "comprehensive", "robust", "seamless", "leverage", "utilize", "facilitate".
- No promotional tone, no filler ("it is important to note that", "in order to"), no sycophantic openers.
- Use straight quotes, not curly.
- Don't create README/CONTRIBUTING/docs files unless explicitly asked.
- Commit messages follow the same rules: clear, direct, no fluff.
