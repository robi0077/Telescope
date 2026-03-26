"""
Fingerprint Bundler
===================
Wraps the PDQ hasher with the canonicalization step.

Canonicalization rule (same as Meta's reference):
  Compute PDQ hash for the original frame AND its horizontal mirror.
  Keep the *lexicographically smaller* hex string as the canonical hash.
  This makes mirrored re-uploads produce the same fingerprint.

Returns a FingerprintBundle that carries:
  - pdq_hash  : canonical 64-char hex string
  - pdq_bits  : canonical 256-bit bool array  (for TMK accumulator)
"""

from __future__ import annotations

import numpy as np
from dataclasses import dataclass

from telescope.fingerprint.hasher import pdq_hash


@dataclass(slots=True)
class FingerprintBundle:
    video_id  : str
    timestamp : float
    pdq_hash  : str           # 64-char hex, canonical (mirror-robust)
    pdq_bits  : np.ndarray    # shape (256,) bool — for TMK accumulation


class Bundler:
    """Stateless factory — safe to share across threads."""

    def create_bundle(
        self,
        video_id  : str,
        timestamp : float,
        image     : np.ndarray,
    ) -> FingerprintBundle:
        """
        Hash the frame and its mirror; keep the canonical (smaller) variant.

        Parameters
        ----------
        video_id  : str   — unique video identifier
        timestamp : float — seconds from start (I-frame PTS)
        image     : ndarray — RGB frame, shape (H, W, 3), uint8
        """
        hex_orig, bits_orig = pdq_hash(image)

        # Horizontal mirror  (np.fliplr is a view — zero copy)
        hex_mirror, bits_mirror = pdq_hash(np.fliplr(image))

        # Canonical = lexicographically smaller hex string
        # (mirrors tend to produce a bit-flipped hash; this collapses them)
        if hex_orig <= hex_mirror:
            canonical_hex  = hex_orig
            canonical_bits = bits_orig
        else:
            canonical_hex  = hex_mirror
            canonical_bits = bits_mirror

        return FingerprintBundle(
            video_id  = video_id,
            timestamp = timestamp,
            pdq_hash  = canonical_hex,
            pdq_bits  = canonical_bits,
        )
