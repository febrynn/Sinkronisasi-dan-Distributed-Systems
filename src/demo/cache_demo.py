import redis
import time
import json
import random

REDIS_HOST = "localhost"
REDIS_PORT = 6379

class CacheNode:
    """Simplified cache node for demo"""
    
    def __init__(self, node_id, redis_client):
        self.node_id = node_id
        self.redis = redis_client
        self.local_cache = {}
        self.cache_stats = {"hits": 0, "misses": 0}
    
    def get(self, key):
        """Get value with cache"""
        # Check local cache first
        if key in self.local_cache:
            self.cache_stats["hits"] += 1
            print(f"[{self.node_id}] Cache HIT: {key}")
            return self.local_cache[key]
        
        # Cache miss - get from Redis
        self.cache_stats["misses"] += 1
        print(f"[{self.node_id}] Cache MISS: {key}, fetching from Redis...")
        
        value = self.redis.get(key)
        if value:
            value = json.loads(value)
            self.local_cache[key] = value
            return value
        return None
    
    def set(self, key, value):
        """Set value and update cache"""
        print(f"[{self.node_id}] Writing: {key}")
        
        # Write to Redis (source of truth)
        self.redis.set(key, json.dumps(value))
        
        # Update local cache
        self.local_cache[key] = value
        
        # Invalidate in other nodes (simplified - just broadcast)
        self._invalidate_others(key)
    
    def _invalidate_others(self, key):
        """Notify other nodes to invalidate their cache"""
        print(f"[{self.node_id}] Broadcasting invalidation for: {key}")
        # In real implementation, this would send RPC to other nodes
        # Here we just publish to Redis pub/sub
        self.redis.publish("cache-invalidate", json.dumps({
            "key": key,
            "from_node": self.node_id
        }))
    
    def invalidate(self, key):
        """Invalidate local cache entry"""
        if key in self.local_cache:
            del self.local_cache[key]
            print(f"[{self.node_id}] Invalidated: {key}")
    
    def get_stats(self):
        """Get cache statistics"""
        total = self.cache_stats["hits"] + self.cache_stats["misses"]
        hit_rate = (self.cache_stats["hits"] / total * 100) if total > 0 else 0
        
        return {
            "node_id": self.node_id,
            "hits": self.cache_stats["hits"],
            "misses": self.cache_stats["misses"],
            "hit_rate": f"{hit_rate:.2f}%",
            "cache_size": len(self.local_cache)
        }

def demo_basic_cache():
    """Demo 1: Basic Cache Operations"""
    print("\n" + "="*60)
    print("DEMO 1: Basic Cache Operations")
    print("="*60)
    
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    r.flushdb()  # Clear all data
    
    node1 = CacheNode("node-1", r)
    
    # Write data
    print("\n[1] Writing user data to cache...")
    node1.set("user:1001", {
        "name": "John Doe",
        "email": "john@example.com",
        "age": 30
    })
    
    time.sleep(0.5)
    
    # Read data (should be cache hit)
    print("\n[2] Reading user data (should be cache HIT)...")
    data = node1.get("user:1001")
    print(f"    Data: {data}")
    
    time.sleep(0.5)
    
    # Read again (should still be hit)
    print("\n[3] Reading again (should be cache HIT)...")
    data = node1.get("user:1001")
    
    # Read non-existent key
    print("\n[4] Reading non-existent key (should be MISS)...")
    data = node1.get("user:9999")
    
    print(f"\n[Stats] {node1.get_stats()}")

def demo_cache_coherence():
    """Demo 2: Cache Coherence Protocol"""
    print("\n" + "="*60)
    print("DEMO 2: Cache Coherence (MESI-like)")
    print("="*60)
    
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    r.flushdb()
    
    node1 = CacheNode("node-1", r)
    node2 = CacheNode("node-2", r)
    node3 = CacheNode("node-3", r)
    
    # Node 1 writes data
    print("\n[1] Node-1 writing product data...")
    node1.set("product:500", {
        "name": "Laptop",
        "price": 1000,
        "stock": 50
    })
    
    time.sleep(1)
    
    # Node 2 reads (cache miss, then cached)
    print("\n[2] Node-2 reading product (MISS → Cached)...")
    data = node2.get("product:500")
    print(f"    Data: {data}")
    
    time.sleep(1)
    
    # Node 2 reads again (cache hit)
    print("\n[3] Node-2 reading again (HIT)...")
    data = node2.get("product:500")
    
    time.sleep(1)
    
    # Node 3 updates the product
    print("\n[4] Node-3 updating product (triggers invalidation)...")
    node3.set("product:500", {
        "name": "Laptop",
        "price": 900,  # Price changed!
        "stock": 45
    })
    
    # Simulate invalidation
    node1.invalidate("product:500")
    node2.invalidate("product:500")
    
    time.sleep(1)
    
    # Node 2 reads again (should be miss, get updated data)
    print("\n[5] Node-2 reading after invalidation (MISS → Updated)...")
    data = node2.get("product:500")
    print(f"    Updated data: {data}")
    print(f"    ✓ Price updated from 1000 to {data['price']}")
    
    print("\n[Stats]")
    for node in [node1, node2, node3]:
        print(f"  {node.get_stats()}")

def demo_performance_comparison():
    """Demo 3: Cache vs No Cache Performance"""
    print("\n" + "="*60)
    print("DEMO 3: Performance Comparison")
    print("="*60)
    
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    r.flushdb()
    
    # Populate some data
    print("\n[Setup] Populating 100 items...")
    for i in range(100):
        r.set(f"item:{i}", json.dumps({"id": i, "value": f"data-{i}"}))
    
    # Test WITH cache
    print("\n[Test 1] Reading 1000 times WITH local cache...")
    node = CacheNode("cached-node", r)
    
    start = time.time()
    for _ in range(1000):
        key = f"item:{random.randint(0, 99)}"
        node.get(key)
    duration_cached = time.time() - start
    
    stats_cached = node.get_stats()
    print(f"    Duration: {duration_cached:.3f}s")
    print(f"    Stats: {stats_cached}")
    
    # Test WITHOUT cache (direct Redis)
    print("\n[Test 2] Reading 1000 times WITHOUT cache (direct Redis)...")
    
    start = time.time()
    for _ in range(1000):
        key = f"item:{random.randint(0, 99)}"
        r.get(key)
    duration_direct = time.time() - start
    
    print(f"    Duration: {duration_direct:.3f}s")
    
    # Comparison
    speedup = duration_direct / duration_cached
    print("\n" + "="*60)
    print(f"RESULT: Cache is {speedup:.2f}x FASTER!")
    print(f"  With cache: {duration_cached:.3f}s")
    print(f"  Without cache: {duration_direct:.3f}s")
    print(f"  Hit rate: {stats_cached['hit_rate']}")
    print("="*60)

def demo_lru_eviction():
    """Demo 4: LRU Cache Eviction"""
    print("\n" + "="*60)
    print("DEMO 4: LRU Cache Eviction")
    print("="*60)
    
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    r.flushdb()
    
    # Simple LRU cache with max size
    class LRUCache:
        def __init__(self, node_id, redis_client, max_size=5):
            self.node_id = node_id
            self.redis = redis_client
            self.cache = {}
            self.max_size = max_size
            self.access_order = []  # LRU tracking
        
        def get(self, key):
            if key in self.cache:
                # Update access order (move to end = most recent)
                self.access_order.remove(key)
                self.access_order.append(key)
                print(f"[{self.node_id}] Cache HIT: {key}")
                return self.cache[key]
            
            print(f"[{self.node_id}] Cache MISS: {key}")
            value = self.redis.get(key)
            if value:
                value = json.loads(value)
                self._add_to_cache(key, value)
                return value
            return None
        
        def _add_to_cache(self, key, value):
            # Evict if at capacity
            if len(self.cache) >= self.max_size:
                evict_key = self.access_order.pop(0)  # Remove least recent
                del self.cache[evict_key]
                print(f"[{self.node_id}] EVICTED: {evict_key} (LRU)")
            
            self.cache[key] = value
            self.access_order.append(key)
            print(f"[{self.node_id}] CACHED: {key} (cache size: {len(self.cache)})")
    
    # Create LRU cache with max 5 items
    lru = LRUCache("lru-node", r, max_size=5)
    
    # Populate Redis with 10 items
    print("\n[Setup] Adding 10 items to Redis...")
    for i in range(10):
        r.set(f"item:{i}", json.dumps({"id": i}))
    
    # Access items 0-4 (fill cache)
    print("\n[Phase 1] Accessing items 0-4 (fill cache)...")
    for i in range(5):
        lru.get(f"item:{i}")
        time.sleep(0.3)
    
    # Access items 5-9 (trigger evictions)
    print("\n[Phase 2] Accessing items 5-9 (trigger evictions)...")
    for i in range(5, 10):
        lru.get(f"item:{i}")
        time.sleep(0.3)
    
    # Try to access item:0 (should be evicted)
    print("\n[Phase 3] Trying to access item:0 (should be evicted)...")
    lru.get("item:0")
    
    print(f"\n[Final] Cache contains: {list(lru.cache.keys())}")

if __name__ == "__main__":
    print("\n" + "="*60)
    print("DISTRIBUTED CACHE DEMO")
    print("="*60)
    print("\nMake sure Redis is running:")
    print("  $ docker-compose up -d redis")
    print("\nStarting demos in 3 seconds...")
    time.sleep(3)
    
    try:
        demo_basic_cache()
        demo_cache_coherence()
        demo_performance_comparison()
        demo_lru_eviction()
        
        print("\n" + "="*60)
        print("DEMO COMPLETED!")
        print("="*60)
    except KeyboardInterrupt:
        print("\n\nDemo interrupted.")
    except Exception as e:
        print(f"\n\nError: {e}")
        import traceback
        traceback.print_exc()