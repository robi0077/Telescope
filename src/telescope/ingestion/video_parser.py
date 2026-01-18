
import av
import logging
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
    gop_structure: List[float] = field(default_factory=list)  # I-frame timestamps

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
                    codec=stream.codec_context.name
                )
                
                # GOP Scan (Packet level only)
                # We iterate packets, identifying Keyframes (I-frames)
                i_frame_timestamps = []
                
                for packet in container.demux(stream):
                    if packet.is_keyframe:
                        # packet.pts is Presentation Time Stamp
                        if packet.pts is not None:
                            timestamp = float(packet.pts * stream.time_base)
                            i_frame_timestamps.append(timestamp)
                            
                metadata.gop_structure = sorted(list(set(i_frame_timestamps)))
                logger.info(f"Parsed {file_path}: {len(metadata.gop_structure)} I-frames found.")
                
                return metadata
                
        except Exception as e:
            logger.error(f"Failed to parse bitstream for {file_path}: {e}")
            raise
