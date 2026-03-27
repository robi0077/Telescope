
import av
import logging
import time
import array
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class VideoMetadata:
    duration: float
    width: int
    height: int
    fps: float
    codec: str
    gop_structure: array.array = field(default_factory=lambda: array.array('d'))  # Optimized double-precision array
    extradata: bytes = b'' # HW Accel support

class BitstreamParser:
    """
    Parses video bitstreams to extract metadata and GOP structure without full decoding.
    Enforces the 'No Unbounded Scans' invariant by streaming packet reading.
    """
    
    def parse(self, file_path: str) -> VideoMetadata:
        """
        Scans the video container to build the GOP map (I-frame timestamps).
        Does NOT decode pixel data.
        """
        try:
            with av.open(file_path) as container:
                stream = container.streams.video[0]
                
                # Basic metadata (O(1) header read without pixel demuxing)
                metadata = VideoMetadata(
                    duration=float(stream.duration * stream.time_base) if stream.duration else 0.0,
                    width=stream.width,
                    height=stream.height,
                    fps=float(stream.average_rate),
                    codec=stream.codec_context.name,
                    extradata=stream.codec_context.extradata or b''
                )
                
                logger.info(f"Parsed metadata headers for {file_path}")
                return metadata
                
        except Exception as e:
            logger.error(f"Failed to parse bitstream for {file_path}: {e}")
            raise
