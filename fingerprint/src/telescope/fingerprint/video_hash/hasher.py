import numpy as np
from scipy.fftpack import dct

def resize_image(image: np.ndarray, size=(32, 32)) -> np.ndarray:
    """
    Robust Resize using Block Averaging (Anti-Aliasing).
    """
    h, w = image.shape[:2]
    h_new, w_new = size
    
    h_block = h // h_new
    w_block = w // w_new
    
    if h_block == 0 or w_block == 0:
         return image[:h_new, :w_new] 

    h_crop = h_block * h_new
    w_crop = w_block * w_new
    
    cropped = image[:h_crop, :w_crop]
    
    if len(image.shape) == 3:
        reshaped = cropped.reshape(h_new, h_block, w_new, w_block, image.shape[2])
        return reshaped.mean(axis=(1, 3)).astype(np.uint8)
    else:
        reshaped = cropped.reshape(h_new, h_block, w_new, w_block)
        return reshaped.mean(axis=(1, 3)).astype(np.uint8)

def rgb_to_gray(image: np.ndarray) -> np.ndarray:
    return np.dot(image[...,:3], [0.2989, 0.5870, 0.1140])

class Hasher:
    @staticmethod
    def structural_hash(image: np.ndarray) -> str:
        small = resize_image(image, size=(32, 32))
        if small.shape[0] != 32 or small.shape[1] != 32:
             small = small[:32, :32]
        gray = rgb_to_gray(small)
        vals = dct(dct(gray, axis=0), axis=1)
        dct_low_freq = vals[0:8, 0:8]
        med = np.median(dct_low_freq)
        hash_bool = dct_low_freq > med
        return Hasher._bool_to_hex(hash_bool.flatten())

    @staticmethod
    def edge_hash(image: np.ndarray) -> str:
        h, w, _ = image.shape
        small = image[::h//8, ::w//9]
        if small.shape[0] > 8: small = small[:8, :]
        if small.shape[1] > 9: small = small[:, :9]
        gray = rgb_to_gray(small)
        diff = gray[:, 1:] > gray[:, :-1]
        return Hasher._bool_to_hex(diff.flatten())

    @staticmethod
    def color_hash(image: np.ndarray) -> str:
        h, w, _ = image.shape
        output = []
        step_h = h // 4
        step_w = w // 4
        for r in range(4):
            for c in range(4):
                chunk = image[r*step_h:(r+1)*step_h, c*step_w:(c+1)*step_w]
                avg = np.mean(chunk, axis=(0,1))
                q = (avg / 64).astype(int) 
                output.append(f"{q[0]}{q[1]}{q[2]}")
        return "".join(output)

    @staticmethod
    def _bool_to_hex(bool_arr: np.ndarray) -> str:
        packed = np.packbits(bool_arr.astype(int))
        return packed.tobytes().hex()

    @staticmethod
    def hamming_distance(hash1: str, hash2: str) -> int:
        h1 = int(hash1, 16)
        h2 = int(hash2, 16)
        return (h1 ^ h2).bit_count()
