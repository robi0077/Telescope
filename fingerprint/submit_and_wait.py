"""
submit_and_wait.py
==================
Submits all video files from TEMP_DIR to the Telescope API,
polls until every job finishes, then exits cleanly.
Called by the system runner after docker-compose is healthy.
"""
import os
import sys
import time
import json
import logging
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("SubmitAndWait")

API_BASE  = "http://localhost:8000"
API_KEY   = "telescope-secret-key-change-me"
HEADERS   = {"x-api-key": API_KEY}
TEMP_DIR  = r"C:\projects\scrapper\temp"
VIDEO_EXTS = {".mp4", ".mkv", ".webm", ".avi", ".mov", ".m4v", ".ts"}
POLL_INTERVAL = 3   # seconds between status checks


def find_videos(root: str):
    """Recursively find all video files."""
    videos = []
    for dirpath, _, filenames in os.walk(root):
        for f in filenames:
            if os.path.splitext(f)[1].lower() in VIDEO_EXTS:
                videos.append(os.path.join(dirpath, f))
    return sorted(videos)


def wait_for_api(timeout=60):
    """Block until the API /status endpoint responds."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"{API_BASE}/status", headers=HEADERS, timeout=3)
            if r.status_code == 200:
                logger.info("API is healthy.")
                return True
        except Exception:
            pass
        logger.info("Waiting for API to start...")
        time.sleep(3)
    return False


def submit_video(path: str) -> dict | None:
    """POST one video to /fingerprint, return {job_id, video_id} or None on error."""
    filename = os.path.basename(path)
    try:
        with open(path, "rb") as fh:
            r = requests.post(
                f"{API_BASE}/fingerprint",
                headers=HEADERS,
                files={"file": (filename, fh, "video/mp4")},
                timeout=60,
            )
        if r.status_code == 200:
            data = r.json()
            logger.info(f"Queued: {filename} → job_id={data['job_id']}")
            return data
        else:
            logger.error(f"Submit failed for {filename}: {r.status_code} {r.text}")
            return None
    except Exception as e:
        logger.error(f"Submit error for {filename}: {e}")
        return None


def poll_jobs(jobs: list[dict]) -> bool:
    """
    Poll Celery task status via Redis until all jobs complete.
    Uses the Celery result endpoint (via /status equivalent).
    Since this API doesn't expose a /task/{id} endpoint,
    we detect completion by watching the prints/ output directory.
    """
    # Collect expected video_ids
    pending = {j["video_id"] for j in jobs if j}
    prints_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "prints"))

    logger.info(f"Polling for {len(pending)} job(s). Watching: {prints_dir}")

    while pending:
        completed = set()
        for video_id in list(pending):
            # Job is done when tmk_vector.json exists (last file written by worker)
            tmk_path = os.path.join(prints_dir, video_id, "video_hash", "tmk_vector.json")
            if os.path.exists(tmk_path):
                logger.info(f"Completed: {video_id}")
                completed.add(video_id)

        pending -= completed

        if pending:
            logger.info(f"Still waiting for {len(pending)}: {list(pending)[:3]}...")
            time.sleep(POLL_INTERVAL)

    logger.info("All jobs completed.")
    return True


def main():
    videos = find_videos(TEMP_DIR)
    if not videos:
        logger.warning(f"No video files found recursively in {TEMP_DIR}. Nothing to do.")
        sys.exit(0)

    logger.info(f"Found {len(videos)} video(s):")
    for v in videos:
        logger.info(f"  {v} ({os.path.getsize(v)/1e6:.1f} MB)")

    if not wait_for_api():
        logger.error("API did not become healthy in time. Is docker-compose running?")
        sys.exit(1)

    # Submit all videos
    jobs = [submit_video(v) for v in videos]
    submitted = [j for j in jobs if j]

    if not submitted:
        logger.error("All submissions failed.")
        sys.exit(1)

    logger.info(f"Submitted {len(submitted)}/{len(videos)} videos. Waiting for completion...")
    poll_jobs(submitted)

    logger.info("Done. All fingerprints saved to prints/")


if __name__ == "__main__":
    main()
