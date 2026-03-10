
import os
import shutil
import logging
from .config import settings

logger = logging.getLogger("TelescopeUtils")

def cleanup_upload_dir():
    """
    Wipes any leftover files from previous crashes to ensure 'Zero Retention' of source video.
    """
    if not os.path.exists(settings.UPLOAD_DIR):
        return

    for filename in os.listdir(settings.UPLOAD_DIR):
        file_path = os.path.join(settings.UPLOAD_DIR, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            logger.warning(f"Failed to delete {file_path}. Reason: {e}")
    
    logger.info("Upload directory cleaned.")

def check_redis_availability() -> bool:
    """
    Checks if Redis is available for Async Worker.
    Returns True if connected, False (and logs warning) if not.
    """
    if not settings.USE_CELERY:
        return False
        
    try:
        import redis
        r = redis.from_url(settings.REDIS_URL, socket_connect_timeout=1)
        r.ping()
        logger.info("Redis connected. Async Worker Enabled.")
        return True
    except Exception as e:
        logger.error(f"Redis not available: {e}")
        return False
