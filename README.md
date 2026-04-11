# ReelPin AI Backend 🚀

The intelligent FastAPI backend driving the **ReelPin** app. This system downloads, transcribes, categorizes, and semantically embeds short-form video content (Instagram Reels) directly into a structured PostgreSQL and Pinecone Vector database.

## Architecture & Pipeline
Whenever a user shares an Instagram Reel to the app, the backend executes the following automated pipeline:

1. **Downloader**: Uses `yt-dlp` to download the HD video and scrape the exact Instagram caption context.
2. **Transcriber**: Uses Llama Whisper (via Groq) to strip the audio off the `.mp4` and generate a near-instant transcript.
3. **AI Extractor**: Passes the combined transcript + caption through an ultra-fast Llama 3 LLM prompt. The AI maps the reel into a massive 46-Category Matrix, extracts key actionable facts, and pulls physical locations.
4. **Geocoding Engine**: Any locations found are piped into the **Google Maps Geocoding API** for exact latitude/longitude extraction (with intelligent neighborhood fallback routing).
5. **Database Core**: The finalized structured payload is written permanently into a Supabase PostgreSQL remote database.
6. **Vector Search (RAG)**: The transcript, category, and summary are embedded and stored in **Pinecone**, allowing users to perform complex natural language queries on their saved reels (e.g. *"Show me that spicy chicken spot from yesterday"*).

## Tech Stack
- **Framework**: FastAPI (Python)
- **AI/LLM Engine**: Groq (Llama 3 70B & Whisper-v3)
- **Database**: Supabase (PostgreSQL)
- **Vector Search**: Pinecone
- **Scraping**: `yt-dlp`
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
   - `SUPABASE_URL` & `SUPABASE_KEY`

5. **Start the Server:**
   ```bash
   uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
   ```
   *Note: Using `--host 0.0.0.0` broadcasts the server over your local Wi-Fi so a physical physical iOS/Android device can consume the backend without getting rejected.*

## API Endpoints Overview

- `GET /api/v1/health` - Check system status.
- `POST /api/v1/process-reel` - Send a raw Instagram Reel URL through the 5-step analysis pipeline.
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
- worker additionally requires a valid `REDIS_URL`

---
*Built tightly for the ReelPin Flutter application.*
