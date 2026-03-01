
from pydantic import BaseModel
from typing import Dict, List, Optional

class MatchResponse(BaseModel):
    is_match: bool
    confidence: float
    video_id: str
    alignment: Dict[str, float]

class StatusResponse(BaseModel):
    status: str
    mode: str
    worker: str
    indexed_videos: int
    version: str

class InventoryResponse(BaseModel):
    videos: List[str]
    count: int

class IngestResponse(BaseModel):
    status: str
    job_id: str
    video_id: str
    message: str
    details: Optional[Dict] = None
