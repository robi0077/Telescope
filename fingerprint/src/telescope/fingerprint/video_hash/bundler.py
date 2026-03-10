from typing import List, Dict
import numpy as np
from .hasher import Hasher

class FingerprintBundle:
    def __init__(self, video_id: str, timestamp: float, variants: Dict[str, str]):
        self.video_id = video_id
        self.timestamp = timestamp
        self.variants = variants

class Bundler:
    def create_bundle(self, video_id: str, timestamp: float, image: np.ndarray) -> FingerprintBundle:
        s_orig = Hasher.structural_hash(image)
        e_orig = Hasher.edge_hash(image)
        
        image_mirror = np.fliplr(image)
        s_mirror = Hasher.structural_hash(image_mirror)
        
        if s_orig < s_mirror:
            canonical_s = s_orig
            canonical_e = e_orig
            canonical_c = Hasher.color_hash(image)
        else:
            canonical_s = s_mirror
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
