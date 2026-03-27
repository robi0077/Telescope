from pydantic import BaseModel
from typing import List, Dict, Any

class StatusResponse(BaseModel):
    status: str
    mode: str
    worker: str
    version: str

class IngestResponse(BaseModel):
    status: str
    job_id: str
    video_id: str
    message: str

class FingerprintResponse(BaseModel):
    video_id: str
    pdq_frames: int
    audio_frames: int
    tmk_dim: int
