
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
                
                # Basic metadata
                metadata = VideoMetadata(
                    duration=float(stream.duration * stream.time_base) if stream.duration else 0.0,
                    width=stream.width,
                    height=stream.height,
                    fps=float(stream.average_rate),
                    codec=stream.codec_context.name,
                    extradata=stream.codec_context.extradata or b''
                )
                
                # GOP Scan (Packet level only)
                # We iterate packets, identifying Keyframes (I-frames)
                # Use array.array for memory efficiency on long 4h+ streams
                i_frame_timestamps = array.array('d')
                
                start_time = time.time()
                TIMEOUT_SECONDS = 60 # Prevent infinite loops on corrupt files
                
                for packet in container.demux(stream):
                    if time.time() - start_time > TIMEOUT_SECONDS:
                        logger.warning(f"Parser timed out scanning {file_path}. Returned partial GOP.")
                        break

                    if packet.is_keyframe:
                        # packet.pts is Presentation Time Stamp
                        if packet.pts is not None:
                            timestamp = float(packet.pts * stream.time_base)
                            i_frame_timestamps.append(timestamp)
                            
                # Deduplicate and sort (though usually sequential)
                # array -> set -> list -> sorted -> array is clunky, but necessary for unique
                # Actually, packet stream is sequential. Just dedup adjacent? 
                # For safety/simplicity:
                unique_ts = sorted(list(set(i_frame_timestamps)))
                metadata.gop_structure = array.array('d', unique_ts)
                
                logger.info(f"Parsed {file_path}: {len(metadata.gop_structure)} I-frames found.")
                
                return metadata
                
        except Exception as e:
            logger.error(f"Failed to parse bitstream for {file_path}: {e}")
            raise
