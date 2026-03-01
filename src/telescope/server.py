from contextlib import asynccontextmanager
from datetime import datetime
import logging
import os
import shutil
from fastapi import FastAPI, UploadFile, File, HTTPException, Depends, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security.api_key import APIKeyHeader

from .config import settings
from .worker import process_video_task
from .core import state
from .models import MatchResponse, StatusResponse, InventoryResponse, IngestResponse
from .utils import cleanup_upload_dir, check_redis_availability

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TelescopeAPI")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    cleanup_upload_dir()
    
    # HARD REQUIREMENT: Redis must be available
    if not check_redis_availability():
        logger.critical("Redis is REQUIRED but not available. Shutting down.")
        raise RuntimeError("Redis connection failed. Telescope requires Redis to run.")
        
    os.makedirs(settings.UPLOAD_DIR, exist_ok=True)
    yield
    # Shutdown (if needed)

app = FastAPI(title="Telescope v4.3 API", lifespan=lifespan)

# Security: API Key only (No Rate Limit)
api_key_header = APIKeyHeader(name="x-api-key", auto_error=True)

async def get_api_key(api_key: str = Security(api_key_header)):
    if api_key != settings.API_KEY: raise HTTPException(403, "Invalid API Key")
    return api_key

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# In-Memory History (Move to DB in Production)
history_log = []

@app.get("/status", response_model=StatusResponse)
def get_status(api_key: str = Depends(get_api_key)):
    return {
        "status": "online", "mode": "production-strict",
        "worker": "celery",
        "indexed_videos": len(state.indexed_videos), "version": "v4.3-prod"
    }

@app.get("/inventory", response_model=InventoryResponse)
def list_inventory(api_key: str = Depends(get_api_key)):
    return {"videos": list(state.indexed_videos), "count": len(state.indexed_videos)}

@app.get("/history")
def list_history(api_key: str = Depends(get_api_key)):
    return history_log

@app.post("/ingest", response_model=IngestResponse)
async def ingest_video(file: UploadFile = File(...), api_key: str = Depends(get_api_key)):
    file_path = os.path.join(settings.UPLOAD_DIR, file.filename)
    try:
        with open(file_path, "wb") as buffer: shutil.copyfileobj(file.file, buffer)
    except Exception as e: raise HTTPException(500, f"Upload failed: {e}")

    logger.info(f"Received {file.filename}. Dispatching...")
    
    try:
        # Always Dispatch - Redis is guaranteed by startup check
        task = process_video_task.delay(file_path, file.filename)
        return {"status": "queued", "job_id": str(task.id), "video_id": file.filename, "message": "Accepted for background processing."}
    except Exception as e:
        logger.error(f"Celery Dispatch Failed: {e}")
        # Even with check, transient errors can happen
        raise HTTPException(500, "Background Worker Queue Failed")

@app.post("/query", response_model=MatchResponse)
async def query_video(file: UploadFile = File(...), api_key: str = Depends(get_api_key)):
    file_path = os.path.join(settings.UPLOAD_DIR, file.filename)
    try:
        with open(file_path, "wb") as buffer: shutil.copyfileobj(file.file, buffer)
        
        # Delegate to Core Brain
        result_obj = state.search_video(file_path, file.filename)
        
        result = MatchResponse(
            is_match=result_obj.is_match, confidence=result_obj.confidence,
            video_id=result_obj.video_id, alignment=result_obj.alignment
        )
        
        history_log.insert(0, {
            "query": file.filename, "match": result.is_match,
            "confidence": result.confidence, "timestamp": datetime.now().strftime("%H:%M:%S")
        })
        return result
    except Exception as e:
        logger.error(f"Query Error: {e}")
        raise HTTPException(500, "Internal Processing Error")
    finally:
        if os.path.exists(file_path): os.remove(file_path)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
