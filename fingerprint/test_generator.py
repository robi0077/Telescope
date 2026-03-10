import sys
import os
import json
import logging

# Ensure src in path to import telescope
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "src")))

from telescope.core import generator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Telescope.Test")

def test_fingerprint_generation():
    sample_file = "sample.mp4"
    video_id = "test_vid_001"
    
    if not os.path.exists(sample_file):
        logger.error(f"Sample file {sample_file} not found. Please download a test video.")
        return

    logger.info(f"Starting test extraction for {sample_file}")
    
    try:
        # Run generation
        metadata, video_fingerprints, audio_fingerprints = generator.extract_fingerprints(sample_file, video_id)
        
        # Log metadata
        logger.info(f"\n--- METADATA ---")
        logger.info(f"Duration: {metadata.duration}s")
        logger.info(f"Resolution: {metadata.width}x{metadata.height}")
        logger.info(f"FPS: {metadata.fps}")
        logger.info(f"Codec: {metadata.codec}")
        logger.info(f"I-Frames Detected (GOP Structure len): {len(metadata.gop_structure)}")
        
        # Log Video Hashes
        logger.info(f"\n--- VIDEO HASHES ({len(video_fingerprints)} extracted) ---")
        for i, vp in enumerate(video_fingerprints[:3]): # Preview first 3
            logger.info(f"VP[{i}] @ {vp['timestamp']:.2f}s | Struct: {vp['structural_hash'][:10]}... | Edge: {vp['edge_hash'][:10]}... | Color: {vp['color_hash'][:10]}...")
        if len(video_fingerprints) > 3: logger.info("...")
            
        # Log Audio Hashes
        logger.info(f"\n--- AUDIO HASHES ({len(audio_fingerprints)} extracted) ---")
        for i, ap in enumerate(audio_fingerprints[:3]): # Preview first 3
            logger.info(f"AP[{i}] @ {ap['timestamp']:.2f}s | Acoustic DCT: {ap['acoustic_hash']}")
        if len(audio_fingerprints) > 3: logger.info("...")
        
        # Ensure outputs are valid JSON
        logger.info("\nChecking JSON serialization...")
        json.dumps(video_fingerprints)
        json.dumps(audio_fingerprints)
        logger.info("JSON serialization OK.")
        
        logger.info("\n✅ Test completed successfully!")
        
    except Exception as e:
        logger.error(f"Test failed with error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_fingerprint_generation()
