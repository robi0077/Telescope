
import os
import logging
from celery import Celery
from .config import settings
from .core import generator
from dataclasses import asdict
import json
import os

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Celery
celery_app = Celery(
    "telescope_worker",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL
)

@celery_app.task(bind=True, name="process_video_task")
def process_video_task(self, file_path: str, video_id: str):
    """
    Background Task: Extracts fingerprints and saves them to a dedicated JSON output folder.
    """
    logger.info(f"[Task {self.request.id}] Starting processing for {video_id}...")
    
    try:
        # Generate fingerprints using the new core logic
        metadata, video_fingerprints, audio_fingerprints = generator.extract_fingerprints(file_path, video_id)
        
        # Create output directory for this specific video
        output_dir = os.path.join(settings.OUTPUT_DIR, video_id)
        video_hash_dir = os.path.join(output_dir, "video_hash")
        audio_hash_dir = os.path.join(output_dir, "audio_hash")
        
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(video_hash_dir, exist_ok=True)
        os.makedirs(audio_hash_dir, exist_ok=True)
        
        # Save Video fingerprints
        video_fingerprint_path = os.path.join(video_hash_dir, "fingerprints.json")
        if video_fingerprints:
            with open(video_fingerprint_path, 'w') as f:
                json.dump(video_fingerprints, f)
                
        # Save Audio fingerprints
        audio_fingerprint_path = os.path.join(audio_hash_dir, "fingerprints.json")
        if audio_fingerprints:
            with open(audio_fingerprint_path, 'w') as f:
                json.dump(audio_fingerprints, f)
            
        # Save metadata (need to handle the array.array specifically or map it, but we can just skip the gop_structure array or format it)
        # To keep it simple for the scraper to read, we filter the raw metadata
        metadata_dict = {
            "duration": metadata.duration,
            "width": metadata.width,
            "height": metadata.height,
            "fps": metadata.fps,
            "codec": metadata.codec
        }
        
        metadata_path = os.path.join(output_dir, "metadata.json")
        with open(metadata_path, 'w') as f:
            json.dump(metadata_dict, f)
            
        logger.info(f"[Task {self.request.id}] Finished. Wrote files to {output_dir}")
        return {"status": "completed", "video_frames": len(video_fingerprints), "audio_frames": len(audio_fingerprints), "video_id": video_id, "output": output_dir}

    except Exception as e:
        logger.error(f"[Task {self.request.id}] Failed: {e}")
        # Re-raise to let Celery handle retries or failure states
        raise e
    finally:
        # Cleanup temp video file
        if os.path.exists(file_path):
            os.remove(file_path)
