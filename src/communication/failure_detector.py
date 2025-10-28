import asyncio
import time

class FailureDetector:
    def __init__(self, peers, interval=2, timeout=5):
        self.peers = peers
        self.interval = interval
        self.timeout = timeout
        self.last_heartbeat = {peer: time.time() for peer in peers}

    async def heartbeat(self, send_heartbeat_func):
        while True:
            await asyncio.gather(*[send_heartbeat_func(peer) for peer in self.peers])
            await asyncio.sleep(self.interval)

    def mark_alive(self, peer):
        self.last_heartbeat[peer] = time.time()

    def check_failures(self):
        now = time.time()
        return [peer for peer, t in self.last_heartbeat.items() if now - t > self.timeout]
