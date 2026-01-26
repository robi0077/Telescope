
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

            for target_ts in metadata.gop_structure:
                # Seek to the timestamp. 
                # 'backward' seek guarantees landing on the I-frame preceding or at the timestamp.
                # Since we are iterating known I-frame timestamps, this should be exact.
                pts = int(target_ts / stream.time_base)
                container.seek(pts, stream=stream, any_frame=False, backward=True)
                
                # Decode the next frame (which should be the I-frame)
                try:
                    for frame in container.decode(stream):
                        # Validate if this is the I-frame we want (or close enough)
                        
                        if frame.key_frame:
                            # DRIFT CORRECTION
                            current_ts = float(frame.pts * stream.time_base)
                            if abs(current_ts - target_ts) > 0.5:
                                logger.warning(f"Seek Drift Detected: Target {target_ts:.2f} vs Actual {current_ts:.2f}. Skipping.")
                                break 

                            # Convert to efficient numpy array (RGB)
                            img_array = frame.to_ndarray(format='rgb24')
                            yield (current_ts, img_array)
                            break # Only want the single I-frame, stop decoding this GOP
                        else:
                            # Should not happen if seek(any_frame=False) works as expected for I-frames
                            continue
                except Exception as e:
                    logger.error(f"Frame Decode Failed at {target_ts}: {e}")
                    continue
