
import numpy as np
import logging
from typing import List, Tuple, Dict, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class MatchResult:
    is_match: bool
    confidence: float
    alignment: Dict[str, float] # slope, offset, etc

class Tier2Verifier:
    """
    Implements the Temporal Proof Engine.
    1. Delta Consensus (Histogram)
    2. Deterministic Pre-Gate
    3. Conditional Robust Regression
    """
    
    def __init__(self, density_threshold: int = 3, regression_tolerance: float = 0.5):
        self.density_threshold = density_threshold
        self.regression_tolerance = regression_tolerance

    def verify_pairs(self, pairs: List[Tuple[float, float]], query_duration: float = 0.0) -> MatchResult:
        """
        pairs: List of (query_timestamp, candidate_timestamp)
        """
        if not pairs:
            return MatchResult(False, 0.0, {})
            
        common_speeds = [1.0, 1.04, 0.96, 1.1, 1.25, 1.5] # Common manipulation speeds
        best_result = MatchResult(False, 0.0, {})
        max_inliers = 0

        for speed in common_speeds:
            # 1. Warp Time: T_cand_normalized = T_cand / speed
            # Delta = (T_cand / speed) - T_query
            # If match is at speed 's', these deltas will be constant.
            warped_pairs = [(q, c/speed) for q, c in pairs]
            
            deltas = [c_norm - q for q, c_norm in warped_pairs]
            
            if not deltas: continue

            # Handle Zero Variance (Single Point or Perfect)
            min_d, max_d = np.min(deltas), np.max(deltas)
            if np.isclose(min_d, max_d):
                hist, bin_edges = np.histogram(deltas, bins=1)
            else:
                hist, bin_edges = np.histogram(deltas, bins='auto')

            # 2. Deterministic Pre-Gate
            peak_idx = np.argmax(hist)
            max_density = hist[peak_idx]
            
            if max_density < self.density_threshold:
                continue
                
            peak_start = bin_edges[peak_idx]
            peak_end = bin_edges[peak_idx+1]
            
            # 3. Inliers
            current_inliers = [(q, c) for (q, c), d in zip(pairs, deltas) if peak_start <= d <= peak_end]
            inlier_count = len(current_inliers)
            
            if inlier_count > max_inliers:
                max_inliers = inlier_count
                
                # Refine Offset (Median of warped residuals)
                # offset = median( (c/s) - q )
                inlier_deltas = [ (c/speed) - q for q, c in current_inliers]
                offset = np.median(inlier_deltas)
                
                # Confidence:
                # User Suggestion: "Raw inlier count" or fixed denominator.
                # We use a sigmoid-like verification: If > 10 good matches, high confidence.
                # Simple linear scaling for MVP: 5 matches = 0.5, 10 matches = 1.0?
                # Or keep ratio but ignore noise?
                # Let's use: inlier_count / (inlier_count + 10) as a robust score that handles noise?
                # No, let's use the USER's specific fix: "Score = Inlier Count" (but normalized to be 0-1 float)
                # We cap it at 20 frames for 1.0 confidence.
                conf = min(inlier_count / 20.0, 1.0)
                
                best_result = MatchResult(
                    is_match=True,
                    confidence=float(conf),
                    alignment={'slope': speed, 'offset': float(offset), 'inliers': inlier_count}
                )

        return best_result
