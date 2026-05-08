# ReelPin AI Backend

The FastAPI backend driving the **ReelPin** app. Instagram-only: this system downloads, transcribes, categorizes, and semantically embeds Instagram reels and image posts into PostgreSQL and Pinecone.

> **Scope:** Instagram URLs only (reels, posts, IGTV). Non-Instagram URLs (YouTube, TikTok, generic web) are rejected with HTTP 400.

## Architecture & Pipeline
When a user shares an Instagram URL, the backend runs:

1. **Downloader**: Public page fetch, then yt-dlp anonymous, then an Apify fallback, then per-cookie-slot authenticated and yt-dlp routes. Image carousels are handled alongside videos.
2. **Transcriber**: Groq Whisper transcribes audio for video posts; image posts go through Groq vision OCR instead.
3. **AI Extractor**: A Groq Llama prompt produces a structured JSON payload (title, summary, content domain, topical tags, key facts, locations, people, actionable items).
4. **Geocoding**: Locations are sent to the Google Maps Geocoding API and cached in Supabase `geocode_cache`.
5. **Database**: The structured payload is written to the Supabase `reels` table.
6. **Vector search**: Title, summary, transcript, and category are embedded (hashed-lexical 384-dim) and upserted into Pinecone for RAG-style natural-language search.

## Tech Stack
- **Framework**: FastAPI (Python)
- **AI/LLM Engine**: Groq (Llama 3 70B & Whisper-v3)
- **Database**: Supabase (PostgreSQL)
- **Vector Search**: Pinecone
- **Scraping**: yt-dlp + direct Instagram page/API + Apify fallback
- **Geocoding**: Google Maps API

## Local Development Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/chetan2921/reelpin-api.git
   cd reelpin-api
   ```

2. **Set up a Virtual Environment:**
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

3. **Install Dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Environment Variables:**
   Create a `.env` file in the root directory duplicating the `.env.example` file layout.
   Inject your personal API keys for:
   - `GROQ_API_KEY`
   - `PINECONE_API_KEY` & `PINECONE_INDEX_NAME`
   - `GOOGLE_MAPS_API_KEY`
   - `SUPABASE_URL` & `SUPABASE_SERVICE_ROLE_KEY`

   For authenticated Instagram downloads, set the active and backup cookie slots:
   - `INSTAGRAM_ACTIVE_COOKIE_DATA_BASE64` or `INSTAGRAM_ACTIVE_COOKIES_FILE`
   - `INSTAGRAM_BACKUP_COOKIE_DATA_BASE64` or `INSTAGRAM_BACKUP_COOKIES_FILE`
   - `INSTAGRAM_TERTIARY_*` for a third standby slot (optional)

   The `YOUTUBE_*`, `TIKTOK_*`, `YTDLP_*`, and `APIFY_*` env fields still exist in `Settings` for backward compatibility but are no longer read by the pipeline.

   Safe cookie rotation steps:
   1. Load the new cookie into the backup slot.
   2. Redeploy and confirm downloads still work.
   3. Promote the backup value into the active slot.
   4. Clear the old backup value after the new active slot is stable.

5. **Start the Server:**
   ```bash
   uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
   ```
   *Note: Using `--host 0.0.0.0` broadcasts the server over your local Wi-Fi so a physical physical iOS/Android device can consume the backend without getting rejected.*

## API Endpoints Overview

- `GET /api/v1/health` - Check system status.
- `POST /api/v1/process-reel` - Send an Instagram URL (reel or post) through the synchronous pipeline.
- `POST /api/v1/processing-jobs/reels` - Queue an Instagram URL for the worker; poll `/api/v1/processing-jobs/{id}` for status.
- `POST /api/v1/process-video` - Manually upload a direct `.mp4` bypassing the scraper.
- `GET /api/v1/reels` - Paginated fetch of saved reels.
- `POST /api/v1/search` - RAG semantic vector search.

## Railway Deploy

Railway may fail to infer a start command automatically because the app entry
point lives under `app/main.py`. This repo includes a `Procfile` with:

```bash
web: python start_service.py
worker: SERVICE_MODE=worker python start_service.py
```

For Railway:
- API service should use `python start_service.py`
- Worker service should use `python start_service.py`
- worker service must set `SERVICE_MODE=worker`
- both services need the same environment variables
- worker polls the `processing_jobs` table directly; `REDIS_URL` is unused
- worker tuning env vars are `WORKER_POLL_INTERVAL_SECONDS`, `WORKER_RECOVERY_INTERVAL_SECONDS`, `WORKER_HEARTBEAT_INTERVAL_SECONDS`, `WORKER_STALE_JOB_MINUTES`, `WORKER_CONCURRENCY`, and `WORKER_INSTAGRAM_CONCURRENCY` (the `_TIKTOK_`, `_YOUTUBE_`, `_WEB_` concurrency vars are no longer read)

---
*Built tightly for the ReelPin Flutter application.*
