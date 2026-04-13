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
    SUPABASE_SERVICE_ROLE_KEY: str | None = None
    SUPABASE_KEY: str | None = None

    # Firebase Cloud Messaging
    FIREBASE_SERVICE_ACCOUNT_PATH: str | None = None
    FIREBASE_SERVICE_ACCOUNT_JSON: str | None = None

    # Queue / worker settings
    REDIS_URL: str = "redis://localhost:6379/0"
    JOB_MAX_RETRIES: int = 3
    JOB_MIN_BACKOFF_MS: int = 15000
    JOB_MAX_BACKOFF_MS: int = 300000
    JOB_FETCH_LIMIT: int = 20
    PROCESSING_JOB_DEFAULT_MAX_ATTEMPTS: int = 3
    USER_SUBMISSION_LIMIT_PER_HOUR: int = 20
    USER_ACTIVE_JOB_LIMIT: int = 4
    WORKER_CONCURRENCY: int = 2
    WORKER_POLL_INTERVAL_SECONDS: float = 3.0
    WORKER_RECOVERY_INTERVAL_SECONDS: int = 60
    WORKER_HEARTBEAT_INTERVAL_SECONDS: int = 15
    WORKER_STALE_JOB_MINUTES: int = 20
    WORKER_TRANSIENT_RETRY_DELAY_SECONDS: int = 60
    WORKER_RATE_LIMIT_RETRY_DELAY_SECONDS: int = 300
    HEALTH_WORKER_STALE_SECONDS: int = 90
    WORKER_INSTAGRAM_CONCURRENCY: int = 1
    WORKER_TIKTOK_CONCURRENCY: int = 1
    WORKER_YOUTUBE_CONCURRENCY: int = 2
    WORKER_WEB_CONCURRENCY: int = 1

    # Downloader auth
    INSTAGRAM_ACTIVE_COOKIES_FILE: str | None = None
    INSTAGRAM_BACKUP_COOKIES_FILE: str | None = None
    INSTAGRAM_ACTIVE_COOKIE_DATA: str | None = None
    INSTAGRAM_ACTIVE_COOKIE_DATA_BASE64: str | None = None
    INSTAGRAM_BACKUP_COOKIE_DATA: str | None = None
    INSTAGRAM_BACKUP_COOKIE_DATA_BASE64: str | None = None
    YOUTUBE_ACTIVE_COOKIE_DATA: str | None = None
    YOUTUBE_ACTIVE_COOKIE_DATA_BASE64: str | None = None
    YOUTUBE_BACKUP_COOKIE_DATA: str | None = None
    YOUTUBE_BACKUP_COOKIE_DATA_BASE64: str | None = None
    TIKTOK_ACTIVE_COOKIE_DATA: str | None = None
    TIKTOK_ACTIVE_COOKIE_DATA_BASE64: str | None = None
    TIKTOK_BACKUP_COOKIE_DATA: str | None = None
    TIKTOK_BACKUP_COOKIE_DATA_BASE64: str | None = None
    YTDLP_ACTIVE_COOKIE_DATA: str | None = None
    YTDLP_ACTIVE_COOKIE_DATA_BASE64: str | None = None
    YTDLP_BACKUP_COOKIE_DATA: str | None = None
    YTDLP_BACKUP_COOKIE_DATA_BASE64: str | None = None
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

    def resolved_supabase_key(self) -> str:
        key = self.SUPABASE_SERVICE_ROLE_KEY or self.SUPABASE_KEY
        if not key:
            raise RuntimeError(
                "Either SUPABASE_SERVICE_ROLE_KEY or legacy SUPABASE_KEY must be configured."
            )
        return key


@lru_cache()
def get_settings() -> Settings:
    return Settings()
