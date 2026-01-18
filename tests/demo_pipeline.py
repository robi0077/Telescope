
import os
import logging
import numpy as np
from telescope.ingestion.video_parser import BitstreamParser, VideoMetadata
from telescope.ingestion.decoder import GOPAlignedDecoder
from telescope.fingerprint.bundler import Bundler
from telescope.tier1.mih import Tier1Index, CandidateAggregator
from telescope.tier2.verifier import Tier2Verifier

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TelescopeDemo")

def run_demo():
    logger.info("Starting Telescope v4.3 Demo Pipeline")
    
    # 1. Setup Components
    parser = BitstreamParser()
    decoder = GOPAlignedDecoder()
    bundler = Bundler()
    index_struct = Tier1Index()
    index_edge = Tier1Index()
    verifier = Tier2Verifier()
    
    # 2. Simulate Ingestion
    # Since we might not have a real video file, we mock the output of Parser/Decoder
    logger.info("Step 1: Ingestion (Mocked)")
    
    # Mock Metadata
    mock_meta = VideoMetadata(duration=100.0, width=1920, height=1080, fps=30.0, codec='h264')
    mock_meta.gop_structure = [0.0, 2.0, 4.0, 6.0, 8.0] # I-frames every 2s
    
    video_id = "vid_123"
    
    # Mock Decoded Frames (Random Noise for demo)
    # In real run: frames = decoder.decode_bundlable_frames("video.mp4", mock_meta)
    frames = []
    for ts in mock_meta.gop_structure:
        # Create random 64x64 image
        img = np.random.randint(0, 255, (64, 64, 3), dtype=np.uint8)
        frames.append((ts, img))
        
    logger.info(f"Ingested {len(frames)} frames for {video_id}")
    
    # 3. Fingerprinting & Indexing
    logger.info("Step 2: Fingerprinting & Indexing")
    for ts, img in frames:
        bundle = bundler.create_bundle(video_id, ts, img)
        
        # Index Signals
        # Structural
        s_hash = bundle.variants['structural']
        index_struct.index(s_hash, video_id, ts)
        
        # Edge
        e_hash = bundle.variants['edge']
        index_edge.index(e_hash, video_id, ts)
        
    logger.info("Indexing Complete.")
    
    # 4. Query (Simulation)
    logger.info("Step 3: Querying")
    # Let's take the first frame and query it
    query_ts, query_img = frames[0]
    query_bundle = bundler.create_bundle("query_vid", 0.0, query_img)
    
    q_s_hash = query_bundle.variants['structural']
    
    # Tier-1 Query
    candidates_struct = index_struct.query(q_s_hash)
    logger.info(f"Tier-1 Candidates (Structural): {candidates_struct}")
    
    # Aggregator
    agg = CandidateAggregator()
    # Mock passing gate (since we query exact same hash, it should match 4 segments)
    passing_vids = agg.filter_candidates(candidates_struct)
    logger.info(f"Passing Tier-1 Gate: {passing_vids}")
    
    if not passing_vids:
        logger.error("No candidates found! (Unexpected for exact match)")
        return

    # 5. Tier-2 Verify
    logger.info("Step 4: Tier-2 Verification")
    candidate_vid = passing_vids[0]
    
    # Mock retrieved pairs (since our index simplified storage, we reconstruct)
    # structure: assuming we retrieved (vid_123, 0.0) from index
    
    # Let's verify (query_ts=0.0) vs (cand_ts=0.0)
    pairs = [(0.0, 0.0)] # Perfect match
    
    # Add some noise/other matches if we had them
    # pairs.append((0.0, 10.0)) # outlier
    
    result = verifier.verify_pairs(pairs)
    logger.info(f"Verification Result: {result}")
    
    if result.is_match:
        logger.info("SUCCESS: Copyright Infringement Detected!")
    else:
        logger.warning("FAILURE: No match verification.")

if __name__ == "__main__":
    run_demo()
