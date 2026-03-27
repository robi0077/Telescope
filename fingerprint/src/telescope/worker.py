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
import logging
import os

from celery import Celery

from .config   import settings
from .core     import generator
from .utils    import add_to_registry, remove_from_registry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Celery app — broker and result backend both on Redis
celery_app = Celery(
    "telescope_worker",
    broker  = settings.REDIS_URL,
    backend = settings.REDIS_URL,
)

# ── TMK metadata ───────────────────────────────────────────────────────────────
_TMK_PERIODS = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]


@celery_app.task(bind=True, name="process_video_task")
def process_video_task(self, file_path: str, video_id: str):
    """
    Background Celery task with Persistent Registry protection.

    1. Registers video as IN-FLIGHT in Redis.
    2. Calls generation pipeline.
    3. Writes all outputs to disk.
    4. ONLY IF SUCCESSFUL: Clears registry and deletes source file.
    """
    logger.info(f"[Task {self.request.id}] Starting PDQF+TMK generation for {video_id}")
    
    # CRASH PROTECTION: Register as in-flight before starting
    add_to_registry(video_id)

    try:
        metadata, per_frame_hashes, audio_hashes, tmk_vector = \
            generator.extract_fingerprints(file_path, video_id)

        # ── Build output directory tree ──────────────────────────────────────
        base_dir      = os.path.join(settings.OUTPUT_DIR, video_id)
        video_hash_dir = os.path.join(base_dir, "video_hash")
        audio_hash_dir = os.path.join(base_dir, "audio_hash")

        os.makedirs(video_hash_dir, exist_ok=True)
        os.makedirs(audio_hash_dir, exist_ok=True)

        # ── Write Outputs ──
        _write_json(os.path.join(base_dir, "metadata.json"), {
            "video_id" : video_id,
            "duration" : metadata.duration,
            "width"    : metadata.width,
            "height"   : metadata.height,
            "fps"      : metadata.fps,
            "codec"    : metadata.codec,
        })

        if per_frame_hashes:
            _write_json(os.path.join(video_hash_dir, "pdq_frames.json"), per_frame_hashes)

        _write_json(os.path.join(video_hash_dir, "tmk_vector.json"), {
            "video_id"  : video_id,
            "num_frames": len(per_frame_hashes),
            "periods"   : _TMK_PERIODS,
            "vector"    : tmk_vector,
        })

        if audio_hashes:
            _write_json(os.path.join(audio_hash_dir, "fingerprints.json"), audio_hashes)

        # ── COMMIT: Cleanup ONLY after successful write ──
        remove_from_registry(video_id)
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Source video {video_id} deleted after successful commit.")

        return {
            "status"     : "completed",
            "video_id"   : video_id,
            "pdq_frames" : len(per_frame_hashes),
            "audio_frames": len(audio_hashes),
            "output"     : base_dir,
        }

    except Exception as e:
        logger.error(f"[Task {self.request.id}] Failed for {video_id}: {e}")
        # NOTE: We DO NOT remove from registry or delete the file on error.
        # This allows for manual audit of why the video failed.
        raise

    finally:
        pass # Deletion logic moved to Success-Path


# ── Utility ────────────────────────────────────────────────────────────────────

def _write_json(path: str, obj) -> None:
    """Atomic-ish JSON write — write then rename to reduce partial-read risk."""
    tmp = path + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(obj, fh)
    os.replace(tmp, path)
