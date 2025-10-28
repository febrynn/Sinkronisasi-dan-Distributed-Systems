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
        self.cache = {}
        self.state = {} 
        self.redis = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

    def read(self, key):
        if key in self.cache and self.state[key] != CacheState.INVALID:
            return self.cache[key]
            
        value = self.redis.get(key)
        if value:
            self.cache[key] = value
            self.state[key] = CacheState.SHARED
        return value

    def write(self, key, value):
        self.cache[key] = value
        self.state[key] = CacheState.MODIFIED
        # Asumsi Redis adalah 'Source of Truth'
        self.redis.set(key, value)
        
    def invalidate(self, key):
        if key in self.state:
            self.state[key] = CacheState.INVALID
            
    def get_status(self):
        return {k: f"{self.cache[k]} ({self.state[k].value})" for k in self.cache}
