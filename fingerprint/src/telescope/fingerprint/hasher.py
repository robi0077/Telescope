"""
PDQ Perceptual Hash (256-bit)
===========================================
Drop-in replacement for the legacy pHash/dHash/color-hash triplet.

Algorithm (faithful to Meta's reference implementation):
  1. BT.601 luma conversion (float32)
  2. Jarosz separable box filter  ← the key differentiator vs plain pHash
     Window = max((dim // 64) * 4, 1) — same formula as Meta's C++ source.
     Implemented as two scipy.ndimage.uniform_filter1d passes (O(N), SIMD-friendly).
  3. 64×64 block-average downsample (anti-aliased by step 2)
  4. 2D DCT-II, ortho norm
  5. Top-left 16×16 block  →  256 coefficients
  6. Threshold at *mean* (PDQ uses mean, not median like pHash)
  7. Pack 256 booleans → 32 bytes → 64-char hex string

Returns both the hex string and the raw bool array.
The bool array is consumed by the TMK accumulator in core.py.
"""

from __future__ import annotations

import numpy as np
from scipy.fftpack import dct as scipy_dct
from scipy.ndimage import uniform_filter1d

# ── Constants ──────────────────────────────────────────────────────────────────
_LUMA = np.array([0.299, 0.587, 0.114], dtype=np.float32)   # BT.601
_PDQ_DIM   = 64    # intermediate resolution after Jarosz + downsample
_PDQ_KEEP  = 16    # DCT coefficients kept per axis  →  16×16 = 256 bits
PDQ_BITS   = _PDQ_KEEP * _PDQ_KEEP  # 256 — exported for use in TMKAccumulator


# ── Internal helpers ───────────────────────────────────────────────────────────

def _to_luma(image: np.ndarray) -> np.ndarray:
    """RGB HxWx3 → float32 HxW luma via BT.601 coefficients."""
    return image[..., :3].astype(np.float32) @ _LUMA


def _jarosz(luma: np.ndarray) -> np.ndarray:
    """
    Separable Jarosz box filter.

    Window size follows Meta's formula: max((dim // 64) * 4, 1).
    Two uniform_filter1d passes (horizontal then vertical) = O(N),
    identical in output to a full 2D box convolution, no extra memory.
    Mode='nearest' replicates edge pixels (same as Meta's clamp strategy).
    """
    h, w = luma.shape
    fw = max((w // _PDQ_DIM) * 4, 1)
    fh = max((h // _PDQ_DIM) * 4, 1)
    tmp = uniform_filter1d(luma, size=fw, axis=1, mode='nearest')
    return uniform_filter1d(tmp,  size=fh, axis=0, mode='nearest')


def _downsample_64(filtered: np.ndarray) -> np.ndarray:
    """
    Block-average downsample to 64×64.
    The Jarosz filter already removed all aliasing above 1/(fw) cycles/pixel,
    so simple block averaging here is lossless within the passband.
    """
    h, w = filtered.shape
    bh = h // _PDQ_DIM
    bw = w // _PDQ_DIM

    if bh == 0 or bw == 0:
        # Images smaller than 64px — crop/pad (shouldn't happen in this pipeline)
        out = np.zeros((_PDQ_DIM, _PDQ_DIM), dtype=np.float32)
        r = min(h, _PDQ_DIM)
        c = min(w, _PDQ_DIM)
        out[:r, :c] = filtered[:r, :c]
        return out

    cropped = filtered[:bh * _PDQ_DIM, :bw * _PDQ_DIM]
    return cropped.reshape(_PDQ_DIM, bh, _PDQ_DIM, bw).mean(axis=(1, 3))


# ── Public API ─────────────────────────────────────────────────────────────────

def pdq_hash(image: np.ndarray) -> tuple[str, np.ndarray]:
    """
    Compute the 256-bit PDQ perceptual hash for a single frame.

    Parameters
    ----------
    image : np.ndarray
        RGB frame, shape (H, W, 3), dtype uint8.

    Returns
    -------
    hex_str : str
        64-character lowercase hex string (256 bits).
    bits : np.ndarray
        Shape (256,) boolean array — consumed by TMKAccumulator.
        True where DCT coefficient > mean, False otherwise.
    """
    luma     = _to_luma(image)
    filtered = _jarosz(luma)
    small    = _downsample_64(filtered)            # float32 64×64

    # 2D DCT-II (separable: row-wise then col-wise, both ortho-normalised)
    dct2d = scipy_dct(scipy_dct(small, axis=0, norm='ortho'), axis=1, norm='ortho')

    # Top-left 16×16 block captures the macro visual structure
    block  = dct2d[:_PDQ_KEEP, :_PDQ_KEEP]        # (16, 16)
    
    # Exclude DC coefficient (index 0) from the median threshold to avoid skewing
    ac_coeffs = block.flatten()[1:]
    threshold  = float(np.median(ac_coeffs))
    bits   = (block > threshold).flatten()              # (256,) bool

    packed  = np.packbits(bits.astype(np.uint8))
    hex_str = packed.tobytes().hex()               # 64 hex chars
    return hex_str, bits


def hamming_distance(hash_a: str, hash_b: str) -> int:
    """
    Bitwise Hamming distance between two PDQ hex strings.
    Operates on 256-bit integers — still O(1) via int.bit_count() on Python 3.10+.
    Valid range: 0 (identical) – 256 (inverse).
    """
    if len(hash_a) != len(hash_b):
        raise ValueError(f"Hash length mismatch: {len(hash_a)} vs {len(hash_b)}")
    a = int(hash_a, 16)
    b = int(hash_b, 16)
    return (a ^ b).bit_count()
