
from collections import defaultdict
from typing import List, Dict, Set, Tuple, Optional
import logging
import redis
import json

from telescope.config import settings

logger = logging.getLogger(__name__)

# Constants
MAX_POSTING_LIST_SIZE = 10000 
SEGMENT_BITS = 16
HASH_BITS = 64
NUM_SEGMENTS = HASH_BITS // SEGMENT_BITS

class Tier1Index:
    """
    Implements MIH with Redis Backend.
    """
    
    def __init__(self):
        # Redis Connection
        try:
            self.r = redis.from_url(settings.REDIS_URL, decode_responses=True)
            self.r.ping() # Check connection
            self.use_redis = True
            logger.info("Connected to Redis for Tier 1 Index.")
        except Exception as e:
            logger.warning(f"Redis not available ({e}). Using IN-MEMORY (RAM) Index (Not persistent).")
            self.use_redis = False
            # Fallback RAM tables: [ { segment_val: [entries] } ]
            self.tables = [defaultdict(list) for _ in range(NUM_SEGMENTS)]
        
    def _split_hash(self, hash_hex: str) -> List[str]:
        if len(hash_hex) != 16: return []
        return [hash_hex[i:i+4] for i in range(0, 16, 4)]
        
    def _get_key(self, segment_idx: int, segment_val: str) -> str:
        return f"v4:t1:{segment_idx}:{segment_val}"

    def index(self, hash_hex: str, video_id: str, timestamp: float):
        segments = self._split_hash(hash_hex)
        payload = f"{video_id}|{timestamp}" 
        
        if self.use_redis:
            # Atomic Lua Script: Only RPUSH if LLEN < Limit
            # ARGV[1] = limit, ARGV[2] = payload
            # KEYS[1] = key
            lua_script = """
            if redis.call('LLEN', KEYS[1]) < tonumber(ARGV[1]) then
                return redis.call('RPUSH', KEYS[1], ARGV[2])
            else
                return 0
            end
            """
            
            pipe = self.r.pipeline()
            for i, val in enumerate(segments):
                key = self._get_key(i, val)
                # Register the script (or eval directly for simplicity in Python redis lib)
                # The lib handles caching if using register_script, but eval is fine for low volume
                pipe.eval(lua_script, 1, key, MAX_POSTING_LIST_SIZE, payload)
                
            pipe.execute()
        else:
            for i, val in enumerate(segments):
                target_list = self.tables[i][val]
                if len(target_list) < MAX_POSTING_LIST_SIZE:
                     target_list.append(payload)

    def query(self, hash_hex: str) -> Dict[str, List[float]]:
        segments = self._split_hash(hash_hex)
        candidates = defaultdict(list)
        
        if self.use_redis:
            pipe = self.r.pipeline()
            keys_to_fetch = []
            
            # 1. Stop-Listing / Dynamic Pruning
            len_pipe = self.r.pipeline()
            for i, val in enumerate(segments):
                key = self._get_key(i, val)
                len_pipe.llen(key)
            
            sizes = len_pipe.execute()
            
            DYNAMIC_PRUNING_LIMIT = 2000 
            
            fetch_indices = []
            for idx, size in enumerate(sizes):
                if size > 0 and size < DYNAMIC_PRUNING_LIMIT:
                    i = idx 
                    val = segments[i]
                    key = self._get_key(i, val)
                    pipe.lrange(key, 0, -1)
                    fetch_indices.append(idx)
            
            if not fetch_indices:
                return candidates

            results = pipe.execute()
            
            # Process results (results is list of lists)
            for raw_entries in results:
                for entry in raw_entries:
                    try:
                        vid, ts_str = entry.split('|')
                        candidates[vid].append(float(ts_str))
                    except ValueError:
                        continue
        else:
            # RAM Fallback
            for i, val in enumerate(segments):
                raw_entries = self.tables[i].get(val, [])
                for entry in raw_entries:
                    try:
                        vid, ts_str = entry.split('|')
                        candidates[vid].append(float(ts_str))
                    except ValueError:
                        continue
                    
        return candidates

class CandidateAggregator:
    def filter_candidates(self, candidates: Dict[str, int]) -> List[str]:
        return [vid for vid, count in candidates.items() if count >= 2]
    
    def cross_signal_gate(self, struct_candidates: Dict[str, int], edge_candidates: Dict[str, int]) -> List[str]:
        final_set = set()
        for vid, count in struct_candidates.items():
            if count >= 2: final_set.add(vid)
        for vid in struct_candidates:
            if vid in edge_candidates: final_set.add(vid)
        return list(final_set)
