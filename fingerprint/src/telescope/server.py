from contextlib import asynccontextmanager
from datetime import datetime
import logging
import os
import shutil
from fastapi import FastAPI, UploadFile, File, HTTPException, Depends, Security
from fastapi.security.api_key import APIKeyHeader

from .config import settings
from .worker import process_video_task
from .core import generator
from .models import StatusResponse, IngestResponse
from .utils import cleanup_upload_dir, check_redis_availability

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TelescopeAPI")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    cleanup_upload_dir()
    
    # HARD REQUIREMENT: Redis must be available for Celery
    if not check_redis_availability():
        logger.critical("Redis is REQUIRED but not available. Shutting down.")
        raise RuntimeError("Redis connection failed. Telescope requires Redis to run.")
        
    os.makedirs(settings.UPLOAD_DIR, exist_ok=True)
    yield

app = FastAPI(title="Telescope v5.0 API (Fingerprinting Node)", lifespan=lifespan)

# Security: API Key only
api_key_header = APIKeyHeader(name="x-api-key", auto_error=True)

async def get_api_key(api_key: str = Security(api_key_header)):
    if api_key != settings.API_KEY: raise HTTPException(403, "Invalid API Key")
    return api_key

@app.get("/status", response_model=StatusResponse)
def get_status(api_key: str = Depends(get_api_key)):
    return {
        "status": "online", "mode": "fingerprint-generator",
        "worker": "celery",
        "version": "v5.0-generator"
    }

@app.post("/fingerprint", response_model=IngestResponse)
def fingerprint_video(file: UploadFile = File(...), api_key: str = Depends(get_api_key)):
    """
    Accepts a video file, saves it to the temp directory, and dispatches a Celery worker 
    to generate the fingerprints to a JSON file.
    """
    file_path = os.path.join(settings.UPLOAD_DIR, file.filename)
    try:
        with open(file_path, "wb") as buffer: shutil.copyfileobj(file.file, buffer)
    except Exception as e: raise HTTPException(500, f"Upload failed: {e}")

    logger.info(f"Received {file.filename}. Dispatching for fingerprint extraction...")
    
    try:
        # Always Dispatch - Redis is guaranteed by startup check
        task = process_video_task.delay(file_path, file.filename)
        return {"status": "queued", "job_id": str(task.id), "video_id": file.filename, "message": "Accepted for background fingerprinting."}
    except Exception as e:
        logger.error(f"Celery Dispatch Failed: {e}")
        raise HTTPException(500, "Background Worker Queue Failed")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
