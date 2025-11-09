import redis
import json
from enum import Enum

class CacheState(Enum):
    MODIFIED = "M"
    EXCLUSIVE = "E"
    SHARED = "S"
    INVALID = "I"

class CacheNode:
    
    def __init__(self, node_id, redis_host="localhost", redis_port=6379):
        self.node_id = node_id
        self.cache = {}  # Local cache storage
        self.state = {}  # Cache state for each key (MESI)
        
        # Connect to Redis (source of truth)
        try:
            self.redis = redis.Redis(
                host=redis_host, 
                port=redis_port, 
                decode_responses=True,
                socket_connect_timeout=5
            )
            # Test connection
            self.redis.ping()
            print(f"[CacheNode-{node_id}] Connected to Redis at {redis_host}:{redis_port}")
        except Exception as e:
            print(f"[CacheNode-{node_id}] ERROR connecting to Redis: {e}")
            raise

    def read(self, key):
        """
        Read value from cache (MESI Read operation).
        If not in cache, fetch from Redis and cache it.
        """
        # Check local cache first
        if key in self.cache and self.state.get(key) != CacheState.INVALID:
            print(f"[CacheNode-{self.node_id}] Cache HIT: {key} (state: {self.state[key].value})")
            
            # If exclusive, transition to shared (read-only)
            if self.state[key] == CacheState.EXCLUSIVE:
                self.state[key] = CacheState.SHARED
            
            return self.cache[key]
        
        # Cache miss - fetch from Redis
        print(f"[CacheNode-{self.node_id}] Cache MISS: {key}")
        
        try:
            value = self.redis.get(key)
            
            if value:
                # Try to parse as JSON
                try:
                    value = json.loads(value)
                except:
                    pass  # Keep as string if not JSON
                
                # Cache the value with SHARED state
                self.cache[key] = value
                self.state[key] = CacheState.SHARED
                
                print(f"[CacheNode-{self.node_id}] Cached {key} with SHARED state")
                return value
            
            return None
            
        except Exception as e:
            print(f"[CacheNode-{self.node_id}] ERROR reading from Redis: {e}")
            return None

    def write(self, key, value):
        """
        Write value to cache (MESI Write operation).
        Writes to Redis and updates local cache with MODIFIED state.
        """
        print(f"[CacheNode-{self.node_id}] Writing: {key}")
        
        try:
            # Serialize value if it's a dict/list
            if isinstance(value, (dict, list)):
                redis_value = json.dumps(value)
            else:
                redis_value = str(value)
            
            # Write to Redis (source of truth)
            self.redis.set(key, redis_value)
            
            # Update local cache with MODIFIED state
            self.cache[key] = value
            self.state[key] = CacheState.MODIFIED
            
            print(f"[CacheNode-{self.node_id}] Cached {key} with MODIFIED state")
            
            # In a real implementation, broadcast invalidation to other nodes
            self._broadcast_invalidation(key)
            
        except Exception as e:
            print(f"[CacheNode-{self.node_id}] ERROR writing to Redis: {e}")
            raise
    
    def _broadcast_invalidation(self, key):
        """
        Broadcast invalidation message to other cache nodes.
        In a real system, this would use pub/sub or RPC.
        """
        try:
            # Publish invalidation message to Redis pub/sub
            message = json.dumps({
                "key": key,
                "from_node": self.node_id,
                "action": "invalidate"
            })
            
            self.redis.publish("cache-invalidate", message)
            print(f"[CacheNode-{self.node_id}] Broadcasted invalidation for {key}")
            
        except Exception as e:
            print(f"[CacheNode-{self.node_id}] ERROR broadcasting invalidation: {e}")
            
    def invalidate(self, key):
        """
        Invalidate a cache entry (MESI Invalidate operation).
        Called when another node writes to the same key.
        """
        if key in self.cache:
            old_state = self.state.get(key, CacheState.INVALID)
            self.state[key] = CacheState.INVALID
            print(f"[CacheNode-{self.node_id}] Invalidated {key} (was {old_state.value})")
        else:
            print(f"[CacheNode-{self.node_id}] Key {key} not in cache, nothing to invalidate")
    
    def evict(self, key):
        """
        Evict a key from cache (for LRU implementation).
        """
        if key in self.cache:
            del self.cache[key]
            if key in self.state:
                del self.state[key]
            print(f"[CacheNode-{self.node_id}] Evicted {key}")
            
    def get_status(self):
        """
        Get current cache status for monitoring.
        """
        status = {}
        for key in self.cache:
            state = self.state.get(key, CacheState.INVALID)
            status[key] = {
                "value": str(self.cache[key])[:50],  # Truncate long values
                "state": state.value
            }
        return status
    
    def get_stats(self):
        """
        Get cache statistics.
        """
        return {
            "node_id": self.node_id,
            "cache_size": len(self.cache),
            "entries": list(self.cache.keys()),
            "states": {k: v.value for k, v in self.state.items()}
        }