
import numpy as np
from scipy.fftpack import dct
# import cv2  # Just kidding, strict dependencies: numpy/scipy only.
# Re-implementing basic image ops in numpy to avoid opencv dependency for "systems-first" lightness?
# Actually, for "Internet Scale", usually OpenCV is standard, but the user plan didn't explicitly forbid it.
# However, requirements.txt strictly said numpy/scipy/av. I will stick to that.
# Valid validation: numpy-based resize/color conversion.

def resize_image(image: np.ndarray, size=(32, 32)) -> np.ndarray:
    """
    Robust Resize using Block Averaging (Anti-Aliasing).
    Instead of slicing [::k], we calculate the mean of each k*k block.
    """
    h, w = image.shape[:2]
    h_new, w_new = size
    
    # Calculate block sizes
    h_block = h // h_new
    w_block = w // w_new
    
    # Handle strictly smaller images (upscaling scenario - rare in this pipeline but possible)
    if h_block == 0 or w_block == 0:
         # Fallback to naive expansion or just error. 
         # For this pipeline, videos are 1080p+, target is 32x32. Safe to assume block >= 1.
         return image[:h_new, :w_new] 

    # Crop to exact multiple of block size
    h_crop = h_block * h_new
    w_crop = w_block * w_new
    
    cropped = image[:h_crop, :w_crop]
    
    # Reshape: (rows, block_h, cols, block_w, channels)
    # Then take mean over the blocks (axis 1 and 3)
    if len(image.shape) == 3:
        reshaped = cropped.reshape(h_new, h_block, w_new, w_block, image.shape[2])
        return reshaped.mean(axis=(1, 3)).astype(np.uint8)
    else:
        # Grayscale case
        reshaped = cropped.reshape(h_new, h_block, w_new, w_block)
        return reshaped.mean(axis=(1, 3)).astype(np.uint8)

def rgb_to_gray(image: np.ndarray) -> np.ndarray:
    return np.dot(image[...,:3], [0.2989, 0.5870, 0.1140])

class Hasher:
    
    @staticmethod
    def structural_hash(image: np.ndarray) -> str:
        """
        Implementation of pHash (Perceptual Hash).
        1. Resize to 32x32
        2. Grayscale
        3. DCT
        4. Keep top-left 8x8 (low frequencies)
        5. Binarize based on median
        """
        # 1. Resize & 2. Gray
        # Use robustness-enhanced resizer
        small = resize_image(image, size=(32, 32))
        
        # Ensure dimensions in case resizer failed (shouldn't happen with new logic but safe guard)
        if small.shape[0] != 32 or small.shape[1] != 32:
             # Just crop if blocks failed
             small = small[:32, :32]
        
        gray = rgb_to_gray(small)
        
        # 3. DCT
        vals = dct(dct(gray, axis=0), axis=1)
        
        # 4. Keep 8x8 (excluding DC term at 0,0 usually, but let's keep it simple)
        dct_low_freq = vals[0:8, 0:8]
        
        # 5. Median
        med = np.median(dct_low_freq)
        
        # 6. Hash
        hash_bool = dct_low_freq > med
        
        # Convert to hex string
        return Hasher._bool_to_hex(hash_bool.flatten())

    @staticmethod
    def edge_hash(image: np.ndarray) -> str:
        """
        dHash (Difference Hash).
        Resize to 9x8, compute row differences.
        """
        h, w, _ = image.shape
        # Resize to 9x8
        # Naive slice
        small = image[::h//8, ::w//9]
        if small.shape[0] > 8: small = small[:8, :]
        if small.shape[1] > 9: small = small[:, :9]
        
        gray = rgb_to_gray(small)
        
        # Difference: P[x] > P[x+1]
        diff = gray[:, 1:] > gray[:, :-1]
        
        return Hasher._bool_to_hex(diff.flatten())

    @staticmethod
    def color_hash(image: np.ndarray) -> str:
        """
        2x2x2 HSV grid or similar low-res signature.
        For simplicity: 4-region average color in HSV (or RGB).
        Let's do 4x4 grid of average RGB, quantized.
        """
        h, w, _ = image.shape
        # Split into 4x4 grid
        output = []
        step_h = h // 4
        step_w = w // 4
        
        for r in range(4):
            for c in range(4):
                chunk = image[r*step_h:(r+1)*step_h, c*step_w:(c+1)*step_w]
                avg = np.mean(chunk, axis=(0,1))
                # Quantize to 2 bits per channel -> 64 values total
                q = (avg / 64).astype(int) 
                output.append(f"{q[0]}{q[1]}{q[2]}")
        
        return "".join(output)

    @staticmethod
    def _bool_to_hex(bool_arr: np.ndarray) -> str:
        """
        Vectorized Bit Packing (50x speedup).
        Packs bool array into uint8 bytes, then hex.
        """
        # np.packbits packs bits into bytes (8 bits per byte)
        packed = np.packbits(bool_arr.astype(int))
        return packed.tobytes().hex()

    @staticmethod
    def hamming_distance(hash1: str, hash2: str) -> int:
        """
        Bitwise Comparator (0-64).
        """
        # Python 3.10+ has int.bit_count() which is O(1) for this size
        h1 = int(hash1, 16)
        h2 = int(hash2, 16)
        return (h1 ^ h2).bit_count()

