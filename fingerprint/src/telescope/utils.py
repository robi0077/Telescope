
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

import time
import redis

def check_redis_availability(retries: int = 15, delay: int = 2) -> bool:
    """
    Checks if Redis is available for Async Worker with retries.
    Returns True if connected, False (and logs warning) if not.
    """
    url = settings.REDIS_URL
    logger.info(f"Checking Redis availability at {url} (up to {retries * delay}s)...")
    
    for i in range(retries):
        try:
            r = redis.from_url(url, socket_connect_timeout=2)
            r.ping()
            logger.info("Redis connected. Async Worker Enabled.")
            return True
        except Exception as e:
            if i < retries - 1:
                logger.info(f"Redis not ready yet (attempt {i+1}/{retries}). Waiting {delay}s...")
                time.sleep(delay)
            else:
                logger.error(f"Redis not available after {retries} attempts: {e}")
                return False
    return False
