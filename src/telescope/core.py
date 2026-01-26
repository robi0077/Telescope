
import logging
import os

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
        
        # Local RAM Index for V1 "Mono-Node" Production (Shared via worker_state hack)
        # In true Distributed, this set is replaced by Redis SISMEMBER checks
        self.indexed_videos = set()

# Singleton Instance
state = SystemState()
