
import os
import logging
from celery import Celery
from .config import settings
from .server import SystemState 

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Celery
celery_app = Celery(
    "telescope_worker",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL
)

# Initialize System State (The "Brain")
# In a real worker, this loads the heavy models into RAM once at startup.
state = SystemState()

@celery_app.task(bind=True, name="process_video_task")
def process_video_task(self, file_path: str, video_id: str):
    """
    Background Task: Encapsulates the heavy Ingestion Logic.
    """
    logger.info(f"[Task {self.request.id}] Starting processing for {video_id}...")
    
    try:
        # 1. Parse Metadata
        # Note: In production, file_path would be an S3 URL, and we'd stream it.
        # For this version, we assume shared filesystem or Docker volume.
        metadata = state.parser.parse(file_path)
        
        # 2. Decode & Hash
        # This is the CPU intensive part that used to block the server.
        processed_count = 0
        for ts, img in state.decoder.decode_bundlable_frames(file_path, metadata):
            bundle = state.bundler.create_bundle(video_id, ts, img)
            
            # 3. Index (Thread-safe in Redis, but here just RAM for now if running local)
            # In a real distributed system, 'state.index_struct' should wrap Redis commands.
            # Since we are "Mocking" the data store with RAM in SystemState for this phase,
            # this worker's RAM is separate from the API's RAM.
            # TO FIX: We need the Index to be external (Redis).
            # For part 1 of refactor, we will just run the logic to prove Async works,
            # knowing that the "Search" won't find it unless we share the RAM or use Redis.
            
            # For the purpose of this "Production Ready" upgrade, 
            # we will realistically just log the success. 
            # The next step "Redis Index" needs to implement the actual Redis storage.
            
            state.index_struct.index(bundle.variants['structural'], video_id, ts)
            state.index_edge.index(bundle.variants['edge'], video_id, ts)
            processed_count += 1
            
        logger.info(f"[Task {self.request.id}] Finished. Processed {processed_count} frames.")
        return {"status": "completed", "frames": processed_count, "video_id": video_id}

    except Exception as e:
        logger.error(f"[Task {self.request.id}] Failed: {e}")
        # Re-raise to let Celery handle retries or failure states
        raise e
    finally:
        # Cleanup temp file
        if os.path.exists(file_path):
            os.remove(file_path)
