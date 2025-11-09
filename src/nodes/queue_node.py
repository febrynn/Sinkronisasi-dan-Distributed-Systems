import redis
import json
import time
from collections import deque

class QueueNode:
    
    def __init__(self, node_id, redis_host="localhost", redis_port=6379):
        self.node_id = node_id
        
        # Connect to Redis
        try:
            self.redis = redis.Redis(
                host=redis_host, 
                port=redis_port, 
                decode_responses=True,
                socket_connect_timeout=5
            )
            self.redis.ping()
            print(f"[QueueNode-{node_id}] Connected to Redis at {redis_host}:{redis_port}")
        except Exception as e:
            print(f"[QueueNode-{node_id}] ERROR connecting to Redis: {e}")
            raise
        
        # Metrics tracking
        self.total_enqueued = 0
        self.total_dequeued = 0
        self.start_time = time.time()
        self.enqueue_timestamps = deque(maxlen=1000)
        self.dequeue_timestamps = deque(maxlen=1000)
        self.topic_stats = {}

    def enqueue(self, topic, message):
        try:
            msg_json = json.dumps(message if isinstance(message, dict) else {"data": message})
            self.redis.rpush(topic, msg_json)
            current_time = time.time()
            self.total_enqueued += 1
            self.enqueue_timestamps.append(current_time)
            if topic not in self.topic_stats:
                self.topic_stats[topic] = {"enqueued": 0, "dequeued": 0, "first_enqueue": current_time}
            self.topic_stats[topic]["enqueued"] += 1
            print(f"[QueueNode-{self.node_id}] Enqueued to {topic} (total: {self.total_enqueued})")
            return {"status": "success", "queue_length": self.redis.llen(topic)}
        except Exception as e:
            print(f"[QueueNode-{self.node_id}] ERROR enqueuing: {e}")
            return {"status": "error", "message": str(e)}

    def dequeue(self, topic, timeout=0):
        try:
            result = self.redis.blpop(topic, timeout=timeout) if timeout>0 else self.redis.lpop(topic)
            if result:
                if isinstance(result, tuple):
                    _, msg_json = result
                else:
                    msg_json = result
                message = json.loads(msg_json)
                current_time = time.time()
                self.total_dequeued += 1
                self.dequeue_timestamps.append(current_time)
                if topic in self.topic_stats:
                    self.topic_stats[topic]["dequeued"] += 1
                print(f"[QueueNode-{self.node_id}] Dequeued from {topic} (total: {self.total_dequeued})")
                return message
            return None
        except Exception as e:
            print(f"[QueueNode-{self.node_id}] ERROR dequeuing: {e}")
            return None

    def calculate_throughput(self, window_seconds=60):
        now = time.time()
        recent_enq = sum(1 for t in self.enqueue_timestamps if t >= now-window_seconds)
        recent_deq = sum(1 for t in self.dequeue_timestamps if t >= now-window_seconds)
        enqueue_rate = recent_enq / window_seconds
        dequeue_rate = recent_deq / window_seconds
        overall = (enqueue_rate + dequeue_rate) / 2
        return {"enqueue_rate": round(enqueue_rate,2),
                "dequeue_rate": round(dequeue_rate,2),
                "overall_msg_per_sec": round(overall,2)}

    def get_stats(self):
        queue_keys = [k.decode() if isinstance(k, bytes) else k for k in self.redis.keys("*-queue")]
        throughput = self.calculate_throughput()
        uptime = round(time.time() - self.start_time,2)
        queue_info = {}
        for key in queue_keys:
            length = self.redis.llen(key)
            topic_stat = self.topic_stats.get(key, {})
            queue_info[key] = {
                "pending": length,
                "enqueued": topic_stat.get("enqueued",0),
                "dequeued": topic_stat.get("dequeued",0),
                "status": "active" if length>0 else "idle"
            }
        return {
            "node_id": self.node_id,
            "uptime_seconds": uptime,
            "total_enqueued": self.total_enqueued,
            "total_dequeued": self.total_dequeued,
            "throughput": throughput,
            "queues": queue_info,
            "timestamp": time.time()
        }
