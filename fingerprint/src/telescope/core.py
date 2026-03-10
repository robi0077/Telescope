import logging
import os
from typing import Dict, List, Tuple, Any

# Core Imports 
try:
    from telescope.ingestion.video_parser import BitstreamParser, VideoMetadata
    from telescope.ingestion.decoder import GOPAlignedDecoder
    from telescope.fingerprint.video_hash.bundler import Bundler
    from telescope.fingerprint.audio_hash.extractor import AcousticExtractor
except ImportError as e:
    logging.getLogger("TelescopeCore").critical(f"CRITICAL: Core dependencies missing ({e}). Cannot start.")
    raise e

logger = logging.getLogger("TelescopeCore")

class FingerprintGenerator:
    """
    The updated pure-generation Brain of the operation. 
    It parses, decodes, and bundles frames, returning the raw hashes.
    """
    def __init__(self):
        self.parser = BitstreamParser()
        self.decoder = GOPAlignedDecoder()
        self.bundler = Bundler()
        self.audio_extractor = AcousticExtractor()

    def extract_fingerprints(self, file_path: str, video_id: str) -> Tuple[VideoMetadata, List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Orchestrates the Generation Pipeline:
        Parse -> Decode -> Bundle -> Return JSON-serializable lists for video and audio
        """
        video_fingerprints = []
        audio_fingerprints = []
        
        # 1. Parse Metadata
        metadata = self.parser.parse(file_path)
        
        # 2. Decode & Hash
        for ts, img in self.decoder.decode_bundlable_frames(file_path, metadata):
            bundle = self.bundler.create_bundle(video_id, ts, img)
            
            # Format the output for the external Database
            video_fingerprints.append({
                "video_id": video_id,
                "timestamp": ts,
                "structural_hash": bundle.variants['structural'],
                "edge_hash": bundle.variants['edge'],
                "color_hash": bundle.variants['color']
            })

        # 3. Audio Extraction
        for ts, hash_val in self.audio_extractor.extract_audio_hashes(file_path):
            audio_fingerprints.append({
                "video_id": video_id,
                "timestamp": ts,
                "acoustic_hash": hash_val
            })

        logger.info(f"Generated {len(video_fingerprints)} video fingerprints and {len(audio_fingerprints)} audio fingerprints for {video_id}.")
        return metadata, video_fingerprints, audio_fingerprints

# Singleton Instance for the Worker
generator = FingerprintGenerator()
