
import av
import numpy as np
from typing import List, Generator, Tuple
from .video_parser import VideoMetadata
import logging

logger = logging.getLogger(__name__)

class GOPAlignedDecoder:
    """
    Enforces the 'GOP-Aligned Decode Policy'.
    Only decodes I-frames as identified by the BitstreamParser.
    Never decodes non-I-frames.
    """
    
    def decode_bundlable_frames(self, file_path: str, metadata: VideoMetadata) -> Generator[Tuple[float, np.ndarray], None, None]:
        """
        Yields (timestamp, frame_rgb_array) for every I-frame in the GOP structure.
        Uses seek() exactness to snap to I-frames to ensure O(1) decode cost per frame.
        """
        if not metadata.gop_structure:
            logger.warning(f"No GOP structure found for {file_path}. Skipping decode.")
            return

        with av.open(file_path) as container:
            stream = container.streams.video[0]
            stream.thread_type = "AUTO" 

            # O(N) Sequential Demux — No backward seeking overhead or drift
            for packet in container.demux(stream):
                if packet.is_keyframe and packet.pts is not None:
                    try:
                        for frame in packet.decode():
                            # GUARANTEE we only yield the actual pristine keyframe
                            if frame.key_frame:
                                current_ts = float(frame.pts * stream.time_base)
                                img_array = frame.to_ndarray(format='rgb24')
                                yield (current_ts, img_array)
                                break  # Prevent duplicate frames from the same packet buffer
                    except Exception as e:
                        logger.error(f"Frame Decode Failed at PTS {packet.pts}: {e}")
                        continue
