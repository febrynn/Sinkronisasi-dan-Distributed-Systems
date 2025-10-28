import redis
import json

class QueueNode:
        
        def __init__(self, node_id, redis_host="localhost", redis_port=6379):
            self.node_id = node_id
            self.redis = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

        def enqueue(self, topic, message):
            # Menggunakan LPUSH/RPUSH pada Redis untuk implementasi Queue
            self.redis.rpush(topic, json.dumps(message))
            return {"status": "success", "message": f"Enqueued to {topic}"}

        def dequeue(self, topic, timeout=0):
            # Menggunakan BLPOP untuk dequeue yang blocking
            if timeout > 0:
                result = self.redis.blpop(topic, timeout=timeout)
                if result:
                    topic_name, msg_json = result
                    return json.loads(msg_json)
                return None
            else:
                msg_json = self.redis.lpop(topic)
                return json.loads(msg_json) if msg_json else None
                
        def get_length(self, topic):
            return self.redis.llen(topic)
