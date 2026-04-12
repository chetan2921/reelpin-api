from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    """Application configuration loaded from environment variables."""

    # Groq
    GROQ_API_KEY: str

    # Google Maps
    GOOGLE_MAPS_API_KEY: str

    # Pinecone
    PINECONE_API_KEY: str
    PINECONE_INDEX_NAME: str = "reelmind"

    # Supabase
    SUPABASE_URL: str
    SUPABASE_KEY: str

    # Firebase Cloud Messaging
    FIREBASE_SERVICE_ACCOUNT_PATH: str | None = None
    FIREBASE_SERVICE_ACCOUNT_JSON: str | None = None

    # Queue / worker settings
    REDIS_URL: str = "redis://localhost:6379/0"
    JOB_MAX_RETRIES: int = 3
    JOB_MIN_BACKOFF_MS: int = 15000
    JOB_MAX_BACKOFF_MS: int = 300000
    JOB_FETCH_LIMIT: int = 20
    WORKER_POLL_INTERVAL_SECONDS: float = 3.0
    WORKER_RECOVERY_INTERVAL_SECONDS: int = 60
    WORKER_STALE_JOB_MINUTES: int = 20

    # Downloader auth
    INSTAGRAM_COOKIES_FILE: str | None = None
    YTDLP_COOKIES_FROM_BROWSER: str | None = None
    YTDLP_COOKIE_DATA: str | None = None
    YTDLP_COOKIE_DATA_BASE64: str | None = None
    INSTAGRAM_COOKIE_DATA: str | None = None
    INSTAGRAM_COOKIE_DATA_BASE64: str | None = None
    YOUTUBE_COOKIE_DATA: str | None = None
    YOUTUBE_COOKIE_DATA_BASE64: str | None = None
    TIKTOK_COOKIE_DATA: str | None = None
    TIKTOK_COOKIE_DATA_BASE64: str | None = None

    # App settings
    TEMP_DOWNLOAD_DIR: str = "/tmp/reelmind_downloads"
    EMBEDDING_MODEL: str = "hashed-lexical-384"
    WHISPER_MODEL: str = "whisper-large-v3-turbo"
    LLM_MODEL: str = "llama-3.3-70b-versatile"
    VISION_MODEL: str = "meta-llama/llama-4-scout-17b-16e-instruct"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache()
def get_settings() -> Settings:
    return Settings()
