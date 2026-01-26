
from typing import List, Dict
import numpy as np
from .hasher import Hasher

class FingerprintBundle:
    def __init__(self, video_id: str, timestamp: float, variants: Dict[str, str]):
        self.video_id = video_id
        self.timestamp = timestamp
        self.variants = variants # e.g., {'structural': 'hash', 'edge': 'hash'}

class Bundler:
    """
    Generates Fingerprint Bundles with canonicalization strategies.
    Covers 'Canonicalization' (Mirrors, Crops).
    """
    
    def create_bundle(self, video_id: str, timestamp: float, image: np.ndarray) -> FingerprintBundle:
        
        # 1. Canonicalization: Generate variants
        # For v4.3 MVP, we focus on the "Original" and "Horizontal Mirror"
        
        # Original
        s_orig = Hasher.structural_hash(image)
        e_orig = Hasher.edge_hash(image)
        
        # Mirror
        image_mirror = np.fliplr(image)
        s_mirror = Hasher.structural_hash(image_mirror)
        
        # Selection Logic:
        # In a real system, we might store ALL, or the "Min" hash to be rotation invariant.
        # Telescope specs says: "Only the canonical minimum representation is stored."
        # This implies we pick the lexicographically smaller hash to represent the frame.
        
        if s_orig < s_mirror:
            canonical_s = s_orig
            canonical_e = e_orig
            # optimization: only compute color for the winner
            canonical_c = Hasher.color_hash(image)
        else:
            canonical_s = s_mirror
            # Note: Edge hash needs to be recomputed for proper mirroring if we want total invariance,
            # or we just store the one corresponding to the structural min.
            canonical_e = Hasher.edge_hash(image_mirror)
            canonical_c = Hasher.color_hash(image_mirror)
            
        return FingerprintBundle(
            video_id=video_id,
            timestamp=timestamp,
            variants={
                'structural': canonical_s,
                'edge': canonical_e,
                'color': canonical_c
            }
        )
