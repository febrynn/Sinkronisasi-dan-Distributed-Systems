import json
import time

class LockManager:
    def __init__(self):
        self.locks = {}
        # Tambahan untuk queue system
        self.queue = []
        self.processed = 0
        self._start_time = time.time()

    # ------------------- BAGIAN LOCK -------------------
    def acquire_lock(self, resource: str, client_id: str, lock_type: str = "exclusive") -> str:
        lock_type = lock_type.lower()
        if resource not in self.locks:
            self.locks[resource] = {
                "holder": [client_id],
                "waiting": [],
                "type": lock_type
            }
            return f"Success: {client_id} acquired {lock_type} lock on {resource}"

        current = self.locks[resource]
        if current["type"] == "shared" and lock_type == "shared":
            if client_id not in current["holder"]:
                current["holder"].append(client_id)
                return f"Success: {client_id} acquired shared lock on {resource}"
            return f"Success: {client_id} already holds shared lock on {resource}"

        current["waiting"].append((client_id, lock_type))
        return f"Waiting: {client_id} waiting for {lock_type} lock on {resource}. Current: {current['type']} by {', '.join(current['holder'])}"

    def release_lock(self, resource: str, client_id: str) -> str:
        if resource not in self.locks:
            return f"Error: Resource '{resource}' is not locked."

        current = self.locks[resource]
        if client_id not in current["holder"]:
            return f"Error: {client_id} does not hold lock on {resource}."

        current["holder"].remove(client_id)
        if current["holder"]:
            if current["type"] == "exclusive":
                return f"Warning: Exclusive lock on {resource} still held by {', '.join(current['holder'])}."
            return f"Success: {client_id} released shared lock on {resource}. Still held by {len(current['holder'])} clients."

        if current["waiting"]:
            new_holders = []
            next_type = current["waiting"][0][1]
            if next_type == "exclusive":
                new_holders.append(current["waiting"].pop(0)[0])
            else:
                i = 0
                while i < len(current["waiting"]) and current["waiting"][i][1] == "shared":
                    new_holders.append(current["waiting"].pop(i)[0])
            current["holder"] = new_holders
            current["type"] = next_type
            return f"Success: {resource} passed to {', '.join(new_holders)} ({next_type} lock)."

        del self.locks[resource]
        return f"Success: {resource} fully unlocked and removed."

    def get_status(self, resource: str = None) -> str:
        if resource and resource in self.locks:
            info = self.locks[resource]
            return json.dumps({
                "resource": resource,
                "type": info["type"],
                "holder": info["holder"],
                "waiting": [w[0] for w in info["waiting"]]
            }, indent=2)
        elif resource:
            return f"Status: Resource '{resource}' is unlocked or unknown."

        return json.dumps({
            res: {
                "type": info["type"],
                "holder": info["holder"],
                "waiting": [w[0] for w in info["waiting"]]
            }
            for res, info in self.locks.items()
        }, indent=2)

    # ------------------- BAGIAN QUEUE -------------------
    def enqueue(self, message: dict):
        self.queue.append(message)
        return {"status": "enqueued", "message": message}

    def process_next(self):
        if not self.queue:
            return {"status": "empty"}
        msg = self.queue.pop(0)
        self.processed += 1
        return {"status": "processed", "message": msg}

    def get_queue_stats(self):
        total_messages = len(self.queue) + self.processed
        pending = len(self.queue)
        uptime = max(time.time() - self._start_time, 1)
        throughput = f"{round(total_messages / uptime, 2)} msg/sec"

        return {
            "total_messages": total_messages,
            "processed": self.processed,
            "pending": pending,
            "throughput": throughput
        }

    def __str__(self):
        return self.get_status()
