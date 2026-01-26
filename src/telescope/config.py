import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Security
    API_KEY: str = "telescope-secret-key-change-me"
    
    # Infrastructure
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    UPLOAD_DIR: str = os.getenv("UPLOAD_DIR", "temp_uploads")
    USE_CELERY: bool = os.getenv("USE_CELERY", "True").lower() == "true"
    
    # Telescope Logic
    # In prod, we might want to tune these
    TIER1_SEGMENT_BITS: int = 16
    
    class Config:
        env_file = ".env"

settings = Settings()
