
import json
import logging
from typing import Optional
from ..fingerprint.bundler import FingerprintBundle

logger = logging.getLogger(__name__)

class BundleStorage:
    """
    Implements 'Signature Co-location'.
    All signals for a fingerprint bundle are stored contiguously.
    """
    
    def store_bundle(self, bundle: FingerprintBundle, path: str):
        # Serialize entire bundle to JSON (or binary)
        # This ensures one disk seek loads Structural, Edge, Color, Motion.
        data = {
            'vid': bundle.video_id,
            'ts': bundle.timestamp,
            'sigs': bundle.variants
        }
        with open(path, 'w') as f:
            json.dump(data, f)
            
    def load_bundle(self, path: str) -> Optional[FingerprintBundle]:
        try:
            with open(path, 'r') as f:
                data = json.load(f)
            return FingerprintBundle(
                video_id=data['vid'],
                timestamp=data['ts'],
                variants=data['sigs']
            )
        except Exception as e:
            logger.error(f"Failed to load bundle: {e}")
            return None
