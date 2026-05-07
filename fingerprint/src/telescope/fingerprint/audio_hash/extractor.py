import av
import numpy as np
import scipy.signal
import scipy.fftpack as fft
import subprocess
import tempfile
import os
import logging

logger = logging.getLogger(__name__)

# Every container/audio extension the consumer or scraper might produce
ALL_AUDIO_EXTENSIONS = (
    # MPEG containers
    '.mp4', '.mp3', '.m4a', '.m4b', '.m4r', '.m4v',
    # MPEG-DASH / HLS segments
    '.m4s', '.ts', '.mts', '.m2ts',
    # WebM / Matroska
    '.webm', '.mkv', '.mka',
    # Ogg family
    '.ogg', '.oga', '.opus',
    # Windows
    '.avi', '.wmv', '.wma', '.asf',
    # Raw / Lossless
    '.flac', '.wav', '.aiff', '.aif', '.aifc', '.au', '.snd',
    # Other
    '.aac', '.ac3', '.eac3', '.dts', '.mp2', '.mp1',
    '.ra', '.rm', '.rmvb', '.3gp', '.3g2',
    '.ape', '.wv', '.mpc', '.tta',
    '.caf', '.amr', '.awb', '.gsm',
    # ALAC in mov
    '.mov',
    # init segments
    '.init',
)


class AcousticExtractor:
    """
    Extracts 64-bit acoustic hashes from ANY audio/video file.
    
    Strategy (in order of preference):
      1. PyAV demux + decode — zero-copy, handles most formats
      2. ffmpeg subprocess pipe — universal fallback for ANY codec PyAV can't decode
         (AC3, DTS, EAC3, Dolby Atmos, rare PCM variants, etc.)
    """

    def __init__(self):
        self.sample_rate = 8000
        self.window_size = self.sample_rate      # 1.0s = 8000 samples
        self.hop_size = self.sample_rate // 2    # 0.5s = 4000 samples
        
        # Silence threshold: Decibels relative to Full Scale (dBFS).
        # Any 1-second chunk quieter than -50 dBFS is considered pure silence 
        # or analog noise floor, and will NOT generate a hash.
        # This prevents completely different silent videos from matching 100%.
        self.silence_threshold_dbfs = -50.0
        
        # Precompute Hanning window to prevent recalculating it for every chunk
        self._hanning_window = np.hanning(self.window_size).astype(np.float32)

    # ─────────────────────────────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────────────────────────────

    def extract_audio_hashes(self, file_path: str):
        """
        Yields (timestamp, hex_hash_str) tuples.
        Tries PyAV streaming first; falls back to ffmpeg subprocess if PyAV fails.
        """
        pyav_generator = self._decode_via_pyav(file_path)
        
        pyav_success = False
        for ts, hash_val in self._hash_samples(pyav_generator):
            pyav_success = True
            yield (ts, hash_val)

        if not pyav_success:
            logger.info(f"PyAV streaming failed or insufficient samples for {file_path}, trying ffmpeg fallback...")
            samples = self._decode_via_ffmpeg(file_path)

            if samples is None or len(samples) < self.window_size:
                logger.warning(f"No usable audio in {file_path} (tried PyAV + ffmpeg)")
                return

            logger.info(f"Extracted {len(samples)} PCM samples from {file_path} using ffmpeg")
            yield from self._hash_samples([samples])

    # ─────────────────────────────────────────────────────────────────────
    # Decoder 1: PyAV  (fast, zero-copy)
    # ─────────────────────────────────────────────────────────────────────

    def _decode_via_pyav(self, file_path: str):
        """
        Decode audio to raw s16 mono 8kHz PCM using PyAV in streaming chunks.
        Yields flat int16 numpy arrays.
        """
        try:
            with av.open(file_path) as container:
                if not container.streams.audio:
                    logger.debug(f"No audio streams in {file_path}")
                    return

                audio_stream = container.streams.audio[0]
                codec_name = audio_stream.codec_context.name
                src_rate = audio_stream.codec_context.sample_rate
                logger.info(
                    f"[PyAV] codec={codec_name} rate={src_rate} "
                    f"layout={audio_stream.codec_context.layout}"
                )

                # Create a fresh resampler per call — stateful object must NOT be reused
                # across files or PyAV throws "Frame does not match Audio format"
                resampler = av.AudioResampler(
                    format='s16',
                    layout='mono',
                    rate=self.sample_rate,
                )

                for packet in container.demux(audio_stream):
                    try:
                        frames = packet.decode()
                    except Exception as e:
                        logger.debug(f"[PyAV] decode error (skipping packet): {e}")
                        continue

                    for frame in frames:
                        frame.pts = None  # reset pts so resampler doesn't complain
                        try:
                            resampled_frames = resampler.resample(frame)
                        except Exception as e:
                            logger.debug(f"[PyAV] resample error (skipping frame): {e}")
                            continue

                        for r_frame in resampled_frames:
                            yield r_frame.to_ndarray().flatten().astype(np.int16)

                # Flush the resampler
                try:
                    for r_frame in resampler.resample(None):
                        yield r_frame.to_ndarray().flatten().astype(np.int16)
                except Exception:
                    pass

        except Exception as e:
            logger.warning(f"[PyAV] Failed to decode {file_path}: {e}")
            return

    # ─────────────────────────────────────────────────────────────────────
    # Decoder 2: ffmpeg subprocess (universal fallback)
    # ─────────────────────────────────────────────────────────────────────

    def _decode_via_ffmpeg(self, file_path: str):
        """
        Uses ffmpeg to decode audio to raw s16le PCM piped to stdout.
        Handles AC3, DTS, EAC3, Dolby Atmos, ALAC, and any other codec
        that ffmpeg supports but PyAV may not decode internally.
        Returns a flat int16 numpy array, or None on failure.
        """
        try:
            cmd = [
                'ffmpeg',
                '-v', 'error',           # suppress noise
                '-i', file_path,
                '-vn',                   # no video
                '-acodec', 'pcm_s16le',  # decode to raw s16 PCM
                '-ar', str(self.sample_rate),  # resample to 8kHz
                '-ac', '1',              # mono
                '-f', 's16le',           # raw format
                'pipe:1',               # pipe to stdout
            ]
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=3600,  # 1 hour timeout (handles 4+ hour movies)
            )

            if result.returncode != 0:
                err = result.stderr.decode('utf-8', errors='replace')[:500]
                logger.warning(f"[ffmpeg] Non-zero exit for {file_path}: {err}")
                return None

            if not result.stdout:
                logger.warning(f"[ffmpeg] No PCM output for {file_path}")
                return None

            samples = np.frombuffer(result.stdout, dtype=np.int16)
            logger.info(f"[ffmpeg] Decoded {len(samples)} samples from {file_path}")
            return samples

        except FileNotFoundError:
            logger.error("[ffmpeg] ffmpeg not found on PATH — cannot use fallback decoder")
            return None
        except subprocess.TimeoutExpired:
            logger.error(f"[ffmpeg] Timeout decoding {file_path}")
            return None
        except Exception as e:
            logger.error(f"[ffmpeg] Unexpected error for {file_path}: {e}")
            return None

    # ─────────────────────────────────────────────────────────────────────
    # DSP: sliding window hash generation
    # ─────────────────────────────────────────────────────────────────────

    def _hash_samples(self, chunk_generator):
        """
        Slide a 1-second window over the PCM chunks with a 0.5-second hop.
        Ignores silent chunks based on RMS/dBFS energy to prevent false-positive matches.
        Yields (timestamp_seconds, hex_hash_str).
        """
        buffer = np.array([], dtype=np.int16)
        absolute_pos = 0

        for chunk in chunk_generator:
            if len(chunk) == 0:
                continue
            buffer = np.concatenate([buffer, chunk])

            while len(buffer) >= self.window_size:
                window = buffer[:self.window_size]
                timestamp = absolute_pos / self.sample_rate

                # ── Silence Filter (RMS & dBFS) ──
                # Calculate the Root Mean Square energy of the window
                # Cast to float64 to prevent overflow when squaring int16s
                rms = np.sqrt(np.mean(window.astype(np.float64) ** 2))
                
                # Convert RMS to Decibels relative to Full Scale (16-bit PCM max = 32768)
                dbfs = 20 * np.log10(rms / 32768.0) if rms > 0 else -float('inf')

                # If the window is quieter than our threshold, it's silence/noise floor. Skip it.
                if dbfs >= self.silence_threshold_dbfs:
                    try:
                        hash_val = self._compute_dct_hash(window)
                        yield (timestamp, hash_val)
                    except Exception as e:
                        logger.debug(f"DSP error at {timestamp:.2f}s: {e}")

                buffer = buffer[self.hop_size:]
                absolute_pos += self.hop_size

    def _compute_dct_hash(self, chunk: np.ndarray) -> str:
        """Transforms 8000 PCM samples into a 64-bit hex hash string."""
        # 1. Hanning window (prevents edge spectral leakage)
        windowed = chunk.astype(np.float32) * self._hanning_window

        # 2. STFT — 129 frequency bands × ~62 time frames
        _, _, Zxx = scipy.signal.stft(windowed, fs=self.sample_rate, nperseg=256)

        # 3. Log-amplitude (volume normalization)
        log_spec = np.log10(np.abs(Zxx) + 1e-9)

        # 4. 2D DCT (JPEG-style compression to macro features)
        dct_mat = fft.dct(fft.dct(log_spec, axis=0, norm='ortho'), axis=1, norm='ortho')

        # 5. Top-left 8×8 captures macro structure — 64 values = 64 bits
        macro = dct_mat[:8, :8]

        # 6. Median binarization
        bits = (macro > np.median(macro)).flatten()

        # 7. Pack to hex (matches visual pHash format)
        return np.packbits(bits.astype(np.uint8)).tobytes().hex()
