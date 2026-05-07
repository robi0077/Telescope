"""
Telescope Core — Fingerprint Orchestration
==========================================
Coordinates the full generation pipeline:
  Parse  →  Decode I-frames  →  PDQ Hash + Bundle  →  TMK accumulate  →  Audio

TMK (Temporal Match Kernel)
---------------------------
Per-frame PDQ hashes form a time-series signal.
A Fourier transform decomposes that signal into frequency components,
producing a fixed-size vector that captures the *visual rhythm* of the video
independently of where you trim it.

Implementation follows Meta's TMK+PDQF paper:
  • 8 periods  : [1, 2, 3, 4, 5, 6, 7, 8] seconds
  • For each period T and each of the 256 PDQ bit positions:
      cos_acc[T,b] += bits[b] * cos(2π * t / T)
      sin_acc[T,b] += bits[b] * sin(2π * t / T)
  • After all frames: average, flatten → 4096 floats, L2-normalise.

Comparison in telescope_db is then a single dot-product (cosine similarity).
"""

from __future__ import annotations

import logging
import mmap
import concurrent.futures
import multiprocessing as mp
from typing import Dict, List, Tuple, Any

import numpy as np

from telescope.fingerprint.hasher import PDQ_BITS

try:
    from telescope.ingestion.video_parser  import BitstreamParser, VideoMetadata
    from telescope.ingestion.decoder       import GOPAlignedDecoder
    from telescope.fingerprint.video_hash.bundler   import Bundler
    from telescope.fingerprint.audio_hash.extractor import AcousticExtractor
except ImportError as e:
    logging.getLogger("TelescopeCore").critical(
        f"CRITICAL: Core dependencies missing ({e}). Cannot start."
    )
    raise

logger = logging.getLogger("TelescopeCore")

# ── TMK constants ──────────────────────────────────────────────────────────────
_TMK_PERIODS: List[float] = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]


class TMKAccumulator:
    """
    Online TMK feature-vector accumulator.

    Design constraints:
      • O(1) memory: never stores raw frame images or a list of bits arrays.
        Only maintains two (8, 256) float64 running sum matrices.
      • Fully vectorised: each add_frame() call is a single NumPy outer-product
        broadcast — no Python loops over periods or bits.
      • Thread-unsafe by design (not needed — one accumulator per video task).
    """

    __slots__ = ("_periods", "_cos_acc", "_sin_acc", "_n")

    def __init__(self, periods: List[float] = _TMK_PERIODS) -> None:
        self._periods = np.asarray(periods, dtype=np.float64)  # shape (P,)
        P = len(periods)
        self._cos_acc = np.zeros((P, PDQ_BITS), dtype=np.float64)
        self._sin_acc = np.zeros((P, PDQ_BITS), dtype=np.float64)
        self._n       = 0

    def add_frame(self, timestamp: float, pdq_bits: np.ndarray) -> None:
        """
        Accumulate one I-frame's contribution.

        Parameters
        ----------
        timestamp : float — frame PTS in seconds
        pdq_bits  : (256,) bool ndarray — canonical PDQ bits from Bundler
        """
        # phases shape: (P,)  — one phase angle per period
        phases = (2.0 * np.pi * timestamp) / self._periods

        # Outer products: (P, 1) * (1, 256) → (P, 256)
        bits_f = pdq_bits.astype(np.float64)                   # (256,)
        self._cos_acc += np.cos(phases)[:, None] * bits_f[None, :]
        self._sin_acc += np.sin(phases)[:, None] * bits_f[None, :]
        self._n += 1

    def compute_vector(self) -> List[float]:
        """
        Finalise and return the normalised TMK feature vector.

        Returns
        -------
        list of 4096 floats (8 periods × 256 bits × 2 for cos+sin),
        L2-normalised so that comparison is a plain dot-product.
        Returns a zero vector if no frames were accumulated.
        """
        if self._n == 0:
            return [0.0] * (len(self._periods) * PDQ_BITS * 2)

        # Average over frames
        cos_avg = self._cos_acc / self._n   # (P, 256)
        sin_avg = self._sin_acc / self._n   # (P, 256)

        # Flatten to 1D: [cos_T1_b0..b255, cos_T2..., sin_T1_b0..b255, sin_T2...]
        vec = np.concatenate(
            [cos_avg.flatten(), sin_avg.flatten()],
            dtype=np.float32
        )

        norm = float(np.linalg.norm(vec))
        if norm > 1e-9:
            vec /= norm

        return vec.tolist()   # JSON-serialisable by worker.py


# ── Main generator ─────────────────────────────────────────────────────────────

def _process_video_worker(file_path: str, video_id: str, metadata: VideoMetadata) -> Tuple[List[Dict[str, Any]], List[float]]:
    decoder = GOPAlignedDecoder()
    bundler = Bundler()
    per_frame_hashes: List[Dict[str, Any]] = []
    tmk = TMKAccumulator()
    for ts, img in decoder.decode_bundlable_frames(file_path, metadata):
        bundle = bundler.create_bundle(video_id, ts, img)
        tmk.add_frame(ts, bundle.pdq_bits)
        per_frame_hashes.append({
            "video_id" : video_id,
            "timestamp": ts,
            "pdq_hash" : bundle.pdq_hash,
        })
    return per_frame_hashes, tmk.compute_vector()

def _process_audio_worker(file_path: str, video_id: str) -> List[Dict[str, Any]]:
    audio_extractor = AcousticExtractor()
    audio_hashes: List[Dict[str, Any]] = []
    for ts, hash_val in audio_extractor.extract_audio_hashes(file_path):
        audio_hashes.append({
            "video_id"    : video_id,
            "timestamp"   : ts,
            "acoustic_hash": hash_val,
        })
    return audio_hashes

class FingerprintGenerator:
    """
    The generation Brain of Telescope.
    Orchestrates: Parse → Decode → PDQ Bundle → TMK accumulate → Audio.
    """

    def __init__(self) -> None:
        self.parser = BitstreamParser()
        self._process_pool = concurrent.futures.ProcessPoolExecutor(
            max_workers=mp.cpu_count()
        )

    def extract_fingerprints(
        self,
        file_path : str,
        video_id  : str,
    ) -> Tuple[VideoMetadata, List[Dict[str, Any]], List[Dict[str, Any]], List[float]]:
        """
        Full generation pipeline using true multi-processing and memory-mapped I/O.
        """
        # Memory-map the file to share I/O OS page cache between video/audio processes
        with open(file_path, 'rb') as f:
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mmapped:
                # 1. Parse bitstream (O(1) header read)
                metadata = self.parser.parse(file_path)

                # 2. Run video and audio in parallel processes
                future_video = self._process_pool.submit(_process_video_worker, file_path, video_id, metadata)
                future_audio = self._process_pool.submit(_process_audio_worker, file_path, video_id)
                
                per_frame_hashes, tmk_vector = future_video.result()
                audio_hashes = future_audio.result()

        logger.info(
            f"[{video_id}] Generated {len(per_frame_hashes)} PDQ frames, "
            f"{len(audio_hashes)} audio hashes, "
            f"TMK vector dim={len(tmk_vector)}."
        )
        return metadata, per_frame_hashes, audio_hashes, tmk_vector


# Singleton — shared across Celery workers in the same process
generator = FingerprintGenerator()
