
import struct
from typing import List, BinaryIO

class VLQ:
    """
    Variable-Length Quantity (VLQ) encoding.
    """
    @staticmethod
    def encode(number: int) -> bytes:
        bytes_list = []
        while True:
            byte = number & 0x7F
            number >>= 7
            if number > 0:
                bytes_list.append(byte | 0x80)
            else:
                bytes_list.append(byte)
                break
        return bytes(reversed(bytes_list))

    @staticmethod
    def decode(stream: BinaryIO) -> int:
        number = 0
        while True:
            byte = stream.read(1)
            if not byte:
                break
            byte = ord(byte)
            number = (number << 7) | (byte & 0x7F)
            if not (byte & 0x80):
                break
        return number

class CompressedIndex:
    """
    Implements 'Compressed Inverted Index' logic.
    Video IDs are delta-encoded and VLQ-compressed.
    """
    
    def save_listing(self, video_ids: List[int], file_path: str):
        video_ids.sort()
        deltas = []
        prev = 0
        for vid in video_ids:
            deltas.append(vid - prev)
            prev = vid
            
        with open(file_path, 'wb') as f:
            for d in deltas:
                f.write(VLQ.encode(d))
                
    def load_listing(self, file_path: str) -> List[int]:
        ids = []
        current = 0
        with open(file_path, 'rb') as f:
            while True:
                # Need careful read handling for VLQ stream
                try:
                    # Very naive consumption of stream here
                    # In production this needs buffered reading
                    # For this mock, we assume we know boundaries or just read one
                    # Actually VLQ.decode reads file byte by byte, which works.
                    delta = VLQ.decode(f)
                    current += delta
                    ids.append(current)
                    
                    # Check EOF (peek?)
                    pos = f.tell()
                    if not f.read(1):
                        break
                    f.seek(pos)
                except Exception:
                    break
        return ids
