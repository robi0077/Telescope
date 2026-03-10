import os
import json
import time
import logging
import redis
from .config import settings
from .core import generator
from .fingerprint.audio_hash.extractor import AcousticExtractor, ALL_AUDIO_EXTENSIONS
import shutil

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] [%(levelname)s] %(message)s",
)
logger = logging.getLogger("Telescope.Consumer")

# Standalone audio extractor for audio-only segments and fallback extraction
_audio_extractor = AcousticExtractor()

# Extensions considered as muxed video containers (they may or may not have embedded audio)
VIDEO_EXTS = (
    '.mp4', '.mkv', '.webm', '.ts', '.mts', '.m2ts', 
    '.avi', '.wmv', '.asf', '.3gp', '.3g2', '.rm', '.rmvb', '.mov', '.m4v'
)
# Audio-only files are everything else in the exhaustive list
AUDIO_ONLY_EXTS = tuple(ext for ext in ALL_AUDIO_EXTENSIONS if ext not in VIDEO_EXTS)


def _extract_audio_hashes_direct(file_path: str, video_id: str) -> list:
    """
    Directly runs AcousticExtractor on a file — works for both:
      - Audio-only containers (.m4s, .m4a, .mp3, etc.)
      - Muxed video containers that have an audio stream
    This is used both for explicit audio files AND as a fallback when
    the full extract_fingerprints pipeline yields 0 audio hashes.
    """
    results = []
    try:
        for ts, hash_val in _audio_extractor.extract_audio_hashes(file_path):
            results.append({
                "video_id": video_id,
                "timestamp": ts,
                "acoustic_hash": hash_val
            })
    except Exception as e:
        logger.error(f"Direct audio extraction failed for {file_path}: {e}")
    return results


class FingerprintConsumer:
    def __init__(self):
        self.redis_client = redis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
        self.queue_name = "queue:to_fingerprint"

        os.makedirs(settings.OUTPUT_DIR, exist_ok=True)
        logger.info(f"Initialized Fingerprint Consumer.")
        logger.info(f"Listening on Queue: {self.queue_name} at {settings.REDIS_URL}")
        logger.info(f"Output Directory: {os.path.abspath(settings.OUTPUT_DIR)}")

    def run(self):
        logger.info("Waiting for videos from the scraper...")
        while True:
            try:
                result = self.redis_client.blpop(self.queue_name, timeout=0)
                if result:
                    _, payload_str = result
                    self.process_envelope(payload_str)
            except Exception as e:
                logger.error(f"Consumer Loop Error: {e}")
                time.sleep(5)

    def process_envelope(self, payload_str):
        video_folder_abs = None
        video_id = "unknown_id"
        try:
            envelope = json.loads(payload_str)
            video_id = envelope.get("task_id", "unknown_id")
            video_folder_rel = envelope.get("video_folder")

            if not video_folder_rel:
                logger.error("Invalid envelope: missing video_folder")
                return

            video_folder_abs = os.path.abspath(os.path.join(settings.UPLOAD_DIR, video_folder_rel))

            if not os.path.isdir(video_folder_abs):
                logger.error(f"Video folder not found: {video_folder_abs}")
                return

            # ── Read the scraper's own metadata.json if it exists ──
            # DASH downloader writes this with explicit 'video_files' and 'audio_files' lists.
            scraper_meta_path = os.path.join(video_folder_abs, "metadata.json")
            scraper_meta = {}
            if os.path.exists(scraper_meta_path):
                try:
                    with open(scraper_meta_path) as f:
                        scraper_meta = json.load(f)
                    logger.info(f"[{video_id}] Scraper metadata found (method={scraper_meta.get('method')})")
                except Exception as e:
                    logger.warning(f"[{video_id}] Could not read scraper metadata.json: {e}")

            scraper_video_files = set(scraper_meta.get("video_files", []))
            scraper_audio_files = set(scraper_meta.get("audio_files", []))
            all_files = [f for f in os.listdir(video_folder_abs) if f != "metadata.json"]

            # ── Classify files ──
            if scraper_video_files or scraper_audio_files:
                # CASE: DASH — scraper told us exactly which files are video vs audio
                video_files = [f for f in all_files if f in scraper_video_files]
                audio_only_files = [f for f in all_files if f in scraper_audio_files]
                logger.info(f"[{video_id}] DASH mode: {len(video_files)} video, {len(audio_only_files)} audio-only files")
            else:
                # CASE: HLS / Progressive — classify by extension
                video_files = [f for f in all_files if f.endswith(VIDEO_EXTS)]
                audio_only_files = [f for f in all_files if f.endswith(AUDIO_ONLY_EXTS)]
                logger.info(f"[{video_id}] HLS/Progressive mode: {len(video_files)} video, {len(audio_only_files)} audio-only files")

            if not video_files and not audio_only_files:
                logger.error(f"[{video_id}] No media files found in {video_folder_abs}")
                return

            # Create output directories
            output_folder = os.path.join(settings.OUTPUT_DIR, video_id)
            video_hash_folder = os.path.join(output_folder, "video_hash")
            audio_hash_folder = os.path.join(output_folder, "audio_hash")
            os.makedirs(output_folder, exist_ok=True)
            os.makedirs(video_hash_folder, exist_ok=True)
            os.makedirs(audio_hash_folder, exist_ok=True)

            start_time = time.time()
            all_video_fingerprints = []
            all_audio_fingerprints = []

            # ─────────────────────────────────────────────────────────────
            # CASE 1 & 2: VIDEO FILES (muxed or video-only DASH segments)
            # ─────────────────────────────────────────────────────────────
            for f in sorted(video_files):
                media_file = os.path.join(video_folder_abs, f)
                logger.info(f"[{video_id}] Processing video segment: {f}")
                try:
                    meta_obj, video_fingerprints, audio_fingerprints = generator.extract_fingerprints(
                        media_file, f"{video_id}_{f}"
                    )
                    
                    if video_fingerprints:
                        all_video_fingerprints.extend(video_fingerprints)

                    # ── CASE 2: Extract audio from muxed video if pipeline got 0 audio hashes ──
                    if not audio_fingerprints:
                        logger.info(f"[{video_id}] No embedded audio from pipeline for {f}, trying direct extraction...")
                        audio_fingerprints = _extract_audio_hashes_direct(media_file, f"{video_id}_{f}")
                        if audio_fingerprints:
                            logger.info(f"[{video_id}] Direct extraction recovered {len(audio_fingerprints)} audio hashes from {f}")

                    if audio_fingerprints:
                        all_audio_fingerprints.extend(audio_fingerprints)

                except Exception as e:
                    logger.error(f"[{video_id}] Failed to process video segment {f}: {e}")

            # ─────────────────────────────────────────────────────────────
            # CASE 3: AUDIO-ONLY FILES (DASH audio .m4s or standalone audio)
            # ─────────────────────────────────────────────────────────────
            for f in sorted(audio_only_files):
                audio_file = os.path.join(video_folder_abs, f)
                logger.info(f"[{video_id}] Processing audio-only segment: {f}")
                try:
                    audio_fingerprints = _extract_audio_hashes_direct(audio_file, f"{video_id}_{f}")
                    if audio_fingerprints:
                        all_audio_fingerprints.extend(audio_fingerprints)
                        logger.info(f"[{video_id}] Extracted {len(audio_fingerprints)} audio hashes from {f}")
                    else:
                        logger.warning(f"[{video_id}] No audio hashes from audio-only file {f}")

                except Exception as e:
                    logger.error(f"[{video_id}] Failed to process audio segment {f}: {e}")

            # ── Aggregate and Save Single JSON Files ──
            if all_video_fingerprints:
                # Sort by timestamp to ensure chronological order across multiple segments
                all_video_fingerprints.sort(key=lambda x: x.get('timestamp', 0))
                vp_path = os.path.join(video_hash_folder, "fingerprints_video.json")
                with open(vp_path, 'w') as fh:
                    json.dump(all_video_fingerprints, fh)
            
            if all_audio_fingerprints:
                all_audio_fingerprints.sort(key=lambda x: x.get('timestamp', 0))
                ap_path = os.path.join(audio_hash_folder, "fingerprints_audio.json")
                with open(ap_path, 'w') as fh:
                    json.dump(all_audio_fingerprints, fh)

            total_video_frames = len(all_video_fingerprints)
            total_audio_frames = len(all_audio_fingerprints)

            # ── Save merged metadata ──
            all_segments = list(video_files) + list(audio_only_files)
            metadata_dict = {
                "source": "telescope",
                "video_id": video_id,
                "original_url": envelope.get("original_url") or scraper_meta.get("link"),
                "site_name": envelope.get("site_name") or scraper_meta.get("site_name"),
                "duration": envelope.get("duration") or scraper_meta.get("duration"),
                "resolution": envelope.get("resolution") or scraper_meta.get("quality"),
                "method": scraper_meta.get("method"),
                "frames_extracted": total_video_frames,
                "audio_frames_extracted": total_audio_frames,
                "video_segments_processed": list(video_files),
                "audio_segments_processed": list(audio_only_files),
                "segments_processed": all_segments,
                "title": envelope.get("title"),
                "uploader": envelope.get("uploader"),
                "thumbnail": envelope.get("thumbnail"),
                "upload_date": envelope.get("upload_date"),
                "view_count": envelope.get("view_count")
            }

            meta_path = os.path.join(output_folder, "metadata.json")
            with open(meta_path, 'w') as fh:
                json.dump(metadata_dict, fh, indent=4)

            elapsed = time.time() - start_time
            logger.info(
                f"[{video_id}] ✓ Done! {total_video_frames} visual frames + "
                f"{total_audio_frames} audio frames in {elapsed:.1f}s"
            )
            logger.info(f"[{video_id}] Saved to: {output_folder}")

        except Exception as e:
            logger.error(f"Failed to process video {video_id}: {e}")
        finally:
            if video_folder_abs and os.path.isdir(video_folder_abs):
                try:
                    shutil.rmtree(video_folder_abs, ignore_errors=True)
                    logger.info(f"[{video_id}] Cleaned up: {video_folder_abs}")
                except Exception as e:
                    logger.warning(f"Cleanup failed for {video_folder_abs}: {e}")


if __name__ == "__main__":
    consumer = FingerprintConsumer()
    consumer.run()
