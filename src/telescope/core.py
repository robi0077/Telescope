import logging
import os
from dataclasses import dataclass
from typing import Dict, List, Tuple

# Core Imports 
# We perform the imports here to centralize the "dependency check" logic
try:
    from telescope.ingestion.video_parser import BitstreamParser, VideoMetadata
    from telescope.ingestion.decoder import GOPAlignedDecoder
    from telescope.fingerprint.bundler import Bundler
    from telescope.tier1.mih import Tier1Index
    from telescope.tier2.verifier import Tier2Verifier
except ImportError as e:
    logging.getLogger("TelescopeCore").critical(f"CRITICAL: Core dependencies missing ({e}). Cannot start SystemState.")
    raise e

logger = logging.getLogger("TelescopeCore")

@dataclass
class MatchResult:
    is_match: bool
    confidence: float
    video_id: str
    alignment: Dict[str, float]

class SystemState:
    """
    The Brain of the operation. Holds instances of all core engines.
    """
    def __init__(self):
        self.parser = BitstreamParser()
        self.decoder = GOPAlignedDecoder()
        self.bundler = Bundler()
        
        # In Production with Redis, these might be lightweight wrappers
        self.index_struct = Tier1Index()
        self.index_edge = Tier1Index()
        
        self.verifier = Tier2Verifier()
        
        
        # Persistence: Load Inventory from Redis
        self.indexed_videos = set()
        if self.index_struct.use_redis:
            # Load existing inventory from Redis
            try:
                inventory = self.index_struct.r.smembers("v4:inventory")
                self.indexed_videos = set(inventory)
                logger.info(f"Loaded {len(self.indexed_videos)} videos from Redis Inventory.")
            except Exception as e:
                logger.error(f"Failed to load inventory from Redis: {e}")
        else:
             logger.warning("Running in RAM-Only mode. Inventory will be lost on restart.")

    def search_video(self, file_path: str, filename: str) -> MatchResult:
        """
        Orchestrates the Search Pipeline:
        Parse -> Decode -> Index Query (Tier 1) -> Verify (Tier 2)
        """
        # 1. Parse Metadata
        metadata = self.parser.parse(file_path)
        
        # 2. Decode & Query (Tier 1)
        candidate_pairs = {} # {vid: [(q_ts, c_ts), ...]}
        
        # Decode Keyframes
        for ts_query, img in self.decoder.decode_bundlable_frames(file_path, metadata):
            bundle = self.bundler.create_bundle(filename, ts_query, img)
            
            # Query Tier 1
            matches = self.index_struct.query(bundle.variants['structural'])
            
            for vid, timestamps in matches.items():
                if vid not in candidate_pairs:
                    candidate_pairs[vid] = []
                for ts_candidate in timestamps:
                    candidate_pairs[vid].append((ts_query, ts_candidate))

        # 3. Filter Candidates (Gate)
        sorted_candidates = sorted(candidate_pairs.items(), key=lambda item: len(item[1]), reverse=True)
        top_candidates = sorted_candidates[:5] 
        
        logger.info(f"Tier 1 found {len(sorted_candidates)} candidates. Verifying Top 5: {[v[0] for v in top_candidates]}")

        # 4. Verify (Tier 2)
        match_found = False
        best_conf = 0.0
        best_vid = ""
        best_align = {}

        for vid, pairs in top_candidates:
             result = self.verifier.verify_pairs(pairs, query_duration=metadata.duration)
             
             if result.is_match:
                 match_found = True
                 best_vid = vid
                 best_conf = result.confidence
                 best_align = result.alignment
                 break 
        
        if match_found:
             return MatchResult(
                is_match=True,
                confidence=best_conf,
                video_id=best_vid,
                alignment=best_align
            )
        else:
             return MatchResult(is_match=False, confidence=0.0, video_id="", alignment={})

# Singleton Instance
state = SystemState()
