"""
Celery Worker — Video Processing Task
======================================
Receives a video file path from the API, runs the full fingerprint generation
pipeline, and writes outputs to the configured OUTPUT_DIR.

Output layout (per video_id):
  {OUTPUT_DIR}/{video_id}/
    metadata.json               ← duration, resolution, fps, codec
    video_hash/
      pdq_frames.json           ← [{video_id, timestamp, pdq_hash}, ...]
      tmk_vector.json           ← {video_id, num_frames, periods, vector}
    audio_hash/
      fingerprints.json         ← [{video_id, timestamp, acoustic_hash}, ...]

The TMK vector and per-frame PDQ hashes are kept separate so telescope_db can
choose to read either or both depending on the matching strategy it applies.
"""

from __future__ import annotations

import json
import os
import tempfile
import time
from pathlib import Path

import av
import structlog
from celery import Celery
from celery.exceptions import SoftTimeLimitExceeded
from prometheus_client import Counter, Histogram, Gauge

from .config   import settings
from .core     import generator
from .utils    import add_to_registry, remove_from_registry

logger = structlog.get_logger()

# Metrics
videos_processed = Counter('telescope_videos_processed_total', 'Total videos processed')
processing_duration = Histogram('telescope_processing_seconds', 'Video processing time')
processing_errors = Counter('telescope_errors_total', 'Processing errors', ['error_type'])
active_tasks = Gauge('telescope_active_tasks', 'Currently processing videos')

# Celery app — broker and result backend both on Redis
celery_app = Celery(
    "telescope_worker",
    broker  = settings.REDIS_URL,
    backend = settings.REDIS_URL,
)

# ── TMK metadata ───────────────────────────────────────────────────────────────
_TMK_PERIODS = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]


@celery_app.task(
    bind=True, 
    name="process_video_task",
    time_limit=3600,      # Hard kill after 1 hour
    soft_time_limit=3300, # Warning at 55 minutes
    max_retries=2,
    autoretry_for=(IOError, av.AVError),
    retry_backoff=True,
)
def process_video_task(self, file_path: str, video_id: str):
    """
    Background Celery task with Persistent Registry protection.

    1. Registers video as IN-FLIGHT in Redis.
    2. Calls generation pipeline.
    3. Writes all outputs to disk.
    4. ONLY IF SUCCESSFUL: Clears registry and deletes source file.
    """
    active_tasks.inc()
    start_time = time.time()
    logger.info("task_started", task_id=self.request.id, video_id=video_id)
    
    # CRASH PROTECTION: Register as in-flight before starting
    if not add_to_registry(video_id):
        active_tasks.dec()
        raise ValueError(f"Duplicate processing detected for {video_id}")

    try:
        metadata, per_frame_hashes, audio_hashes, tmk_vector = \
            generator.extract_fingerprints(file_path, video_id)

        # ── Build output directory tree ──────────────────────────────────────
        base_dir      = os.path.join(settings.OUTPUT_DIR, video_id)
        # ── Write Outputs ──
        # Instead of 4 separate writes, create one master document
        output_data = {
            "metadata": {
                "video_id": video_id,
                "duration": metadata.duration,
                "width": metadata.width,
                "height": metadata.height,
                "fps": metadata.fps,
                "codec": metadata.codec,
            },
            "video_hash": {
                "pdq_frames": per_frame_hashes,
                "tmk_vector": {
                    "num_frames": len(per_frame_hashes),
                    "periods": _TMK_PERIODS,
                    "vector": tmk_vector,
                }
            },
            "audio_hash": {
                "fingerprints": audio_hashes
            }
        }

        # Single atomic write
        _write_json(os.path.join(base_dir, "fingerprints.json"), output_data)

        # ── COMMIT: Cleanup ONLY after successful write ──
        remove_from_registry(video_id)
        if os.path.exists(file_path):
            os.remove(file_path)
            
        logger.info(
            "fingerprints_generated",
            video_id=video_id,
            pdq_frames=len(per_frame_hashes),
            audio_frames=len(audio_hashes),
            tmk_dim=len(tmk_vector),
            duration_seconds=time.time() - start_time
        )
        
        videos_processed.inc()
        processing_duration.observe(time.time() - start_time)

        return {
            "status"     : "completed",
            "video_id"   : video_id,
            "pdq_frames" : len(per_frame_hashes),
            "audio_frames": len(audio_hashes),
            "output"     : base_dir,
        }

    except SoftTimeLimitExceeded as e:
        logger.warning("task_timeout_approaching", video_id=video_id)
        remove_from_registry(video_id)
        if os.path.exists(file_path):
            os.remove(file_path)
        processing_errors.labels(error_type=type(e).__name__).inc()
        raise
    except Exception as e:
        logger.error("task_failed", task_id=self.request.id, video_id=video_id, error=str(e))
        processing_errors.labels(error_type=type(e).__name__).inc()
        # NOTE: We DO NOT remove from registry or delete the file on error.
        # This allows for manual audit of why the video failed.
        raise

    finally:
        active_tasks.dec()


# ── Utility ────────────────────────────────────────────────────────────────────

def _write_json(path: str, obj) -> None:
    """Atomic write using same-filesystem temp file."""
    path_obj = Path(path)
    
    # Create temp in SAME directory (required for atomic replace)
    with tempfile.NamedTemporaryFile(
        mode='w',
        dir=path_obj.parent,
        delete=False,
        prefix='.tmp_',
        suffix='.json'
    ) as tmp:
        json.dump(obj, tmp, indent=2)
        tmp_name = tmp.name
    
    try:
        # Force flush to disk before rename
        os.fsync(tmp.fileno()) if hasattr(tmp, 'fileno') else None
        
        # Atomic rename (POSIX) / best-effort (Windows)
        if os.name == 'nt':  # Windows
            if path_obj.exists():
                path_obj.unlink()
        os.replace(tmp_name, path)
    except Exception as e:
        Path(tmp_name).unlink(missing_ok=True)
        raise
