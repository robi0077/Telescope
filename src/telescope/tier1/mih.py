
from collections import defaultdict
from typing import List, Dict, Set, Tuple, Optional
import logging

logger = logging.getLogger(__name__)

# Constants
MAX_POSTING_LIST_SIZE = 10000 # Zipf protection threshold
SEGMENT_BITS = 16
HASH_BITS = 64
NUM_SEGMENTS = HASH_BITS // SEGMENT_BITS

class PostingList:
    def __init__(self):
        self.entries: List[Tuple[str, float]] = [] # (video_id, timestamp)
        self.is_stop_listed: bool = False

    def add(self, video_id: str, timestamp: float):
        if self.is_stop_listed:
            return
        
        self.entries.append((video_id, timestamp))
        
        if len(self.entries) > MAX_POSTING_LIST_SIZE:
            self.is_stop_listed = True
            self.entries = [] # Flush to free memory, permanently ignored
            logger.warning(f"PostingList hit limits. Marked as Stop-Listed.")

class Tier1Index:
    """
    Implements MIH with 4 x 16-bit segments.
    Enforces 'Zipf’s Law Protection'.
    """
    
    def __init__(self):
        # 4 Tables (one per segment)
        # Table -> {segment_value: PostingList}
        self.tables: List[Dict[str, PostingList]] = [defaultdict(PostingList) for _ in range(NUM_SEGMENTS)]
        
    def _split_hash(self, hash_hex: str) -> List[str]:
        # Hash is 64-bit hex (16 chars)
        # Split into 4 chunks of 4 chars
        if len(hash_hex) != 16:
            # Handle non-compliant hashes or fallback
            return []
        
        return [hash_hex[i:i+4] for i in range(0, 16, 4)]

    def index(self, hash_hex: str, video_id: str, timestamp: float):
        segments = self._split_hash(hash_hex)
        for i, val in enumerate(segments):
            self.tables[i][val].add(video_id, timestamp)

    def query(self, hash_hex: str) -> Dict[str, int]:
        """
        Returns candidates and their match counts.
        candidates: {video_id: number_of_segment_matches}
        
        Enforces 'k-of-n Admission Gate' logic partially here 
        (count returned, filtering usually happens at aggregator).
        """
        segments = self._split_hash(hash_hex)
        candidates = defaultdict(int)
        
        for i, val in enumerate(segments):
            plist = self.tables[i].get(val)
            if plist and not plist.is_stop_listed:
                for vid, _ in plist.entries:
                    candidates[vid] += 1
        
        return candidates

class CandidateAggregator:
    """
    Applies the k-of-n gate policy.
    Gate: >= 2 segments match.
    """
    
    def filter_candidates(self, candidates: Dict[str, int]) -> List[str]:
        # Basic k=2 rule on a SINGLE signal (structural usually)
        return [vid for vid, count in candidates.items() if count >= 2]
    
    def cross_signal_gate(self, struct_candidates: Dict[str, int], edge_candidates: Dict[str, int]) -> List[str]:
        # OR >= 1 segment match across >= 2 different signals
        # This requires slightly more complex logic tracking per-signal hits.
        # For v4.3 MVP, we implement the basic logic.
        final_set = set()
        
        # Rule 1: >=2 in Structural
        for vid, count in struct_candidates.items():
            if count >= 2:
                final_set.add(vid)
                
        # Rule 2: >=1 in Struct AND >=1 in Edge
        for vid in struct_candidates:
            if vid in edge_candidates:
                final_set.add(vid)
                
        return list(final_set)
