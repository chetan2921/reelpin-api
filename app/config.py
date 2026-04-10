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

    # App settings
    TEMP_DOWNLOAD_DIR: str = "/tmp/reelmind_downloads"
    EMBEDDING_MODEL: str = "all-MiniLM-L6-v2"
    WHISPER_MODEL: str = "whisper-large-v3-turbo"
    LLM_MODEL: str = "llama-3.3-70b-versatile"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache()
def get_settings() -> Settings:
    return Settings()
