import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Security
    API_KEY: str = "telescope-secret-key-change-me"
    
    # Infrastructure
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6666/0")
    UPLOAD_DIR: str = os.getenv("UPLOAD_DIR", r"C:\projects\scrapper\temp")
    OUTPUT_DIR: str = os.getenv("OUTPUT_DIR", r"..\prints")
    USE_CELERY: bool = os.getenv("USE_CELERY", "False").lower() == "true"
    
    # Telescope Logic
    # In prod, we might want to tune these
    TIER1_SEGMENT_BITS: int = 16
    
    class Config:
        env_file = ".env"

settings = Settings()
