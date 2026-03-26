"""
Test: Fingerprint Generation Pipeline (PDQF + TMK)
====================================================
Exercises the full generation stack end-to-end against a real video file.
Run from the fingerprint/ directory:
    python test_generator.py <path-to-video.mp4>

No sample file path = looks for 'sample.mp4' in CWD.
"""

import sys
import os
import json
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "src")))

from telescope.core import generator

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger("Telescope.Test")


def test_fingerprint_generation(sample_file: str = "sample.mp4") -> None:
    video_id = os.path.splitext(os.path.basename(sample_file))[0]

    if not os.path.exists(sample_file):
        logger.error(f"Sample file not found: {sample_file}")
        sys.exit(1)

    logger.info(f"Starting PDQF+TMK extraction for '{sample_file}'")

    try:
        metadata, per_frame_hashes, audio_hashes, tmk_vector = \
            generator.extract_fingerprints(sample_file, video_id)

        # ── Metadata ─────────────────────────────────────────────────────────
        logger.info("--- VIDEO METADATA ---")
        logger.info(f"  Duration   : {metadata.duration:.2f}s")
        logger.info(f"  Resolution : {metadata.width}x{metadata.height}")
        logger.info(f"  FPS        : {metadata.fps:.2f}")
        logger.info(f"  Codec      : {metadata.codec}")
        logger.info(f"  I-Frames   : {len(metadata.gop_structure)}")

        # ── PDQ per-frame hashes ──────────────────────────────────────────────
        logger.info(f"\n--- PDQ FRAME HASHES ({len(per_frame_hashes)} frames) ---")
        for fp in per_frame_hashes[:3]:
            logger.info(
                f"  t={fp['timestamp']:7.2f}s | pdq={fp['pdq_hash'][:16]}..."
            )
        if len(per_frame_hashes) > 3:
            logger.info(f"  ... ({len(per_frame_hashes) - 3} more frames)")

        # ── TMK vector ────────────────────────────────────────────────────────
        logger.info(f"\n--- TMK VECTOR ---")
        logger.info(f"  Dimension  : {len(tmk_vector)}")           # expect 4096
        logger.info(f"  First 4    : {tmk_vector[:4]}")
        import math
        norm = math.sqrt(sum(v * v for v in tmk_vector))
        logger.info(f"  L2 norm    : {norm:.6f}")                  # should be ~1.0

        # ── Audio hashes ──────────────────────────────────────────────────────
        logger.info(f"\n--- AUDIO HASHES ({len(audio_hashes)} windows) ---")
        for ap in audio_hashes[:3]:
            logger.info(
                f"  t={ap['timestamp']:7.2f}s | acoustic={ap['acoustic_hash']}"
            )
        if len(audio_hashes) > 3:
            logger.info(f"  ... ({len(audio_hashes) - 3} more windows)")

        # ── JSON serialisability check ────────────────────────────────────────
        json.dumps(per_frame_hashes)
        json.dumps(audio_hashes)
        json.dumps(tmk_vector)
        logger.info("\n✅ JSON serialisation OK")
        logger.info("✅ Test completed successfully")

    except Exception:
        logger.exception("Test failed")
        sys.exit(1)


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "sample.mp4"
    test_fingerprint_generation(path)
