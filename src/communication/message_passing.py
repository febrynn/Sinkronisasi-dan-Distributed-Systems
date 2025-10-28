import aiohttp
import asyncio
import json

class MessagePassing:
    def __init__(self, peers):
        self.peers = peers  # list of node addresses

    async def send_message(self, peer, message):
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(f"http://{peer}/message", json=message) as resp:
                    return await resp.json()
            except Exception as e:
                print(f"Failed to send message to {peer}: {e}")
                return None

    async def broadcast(self, message):
        tasks = [self.send_message(peer, message) for peer in self.peers]
        return await asyncio.gather(*tasks)
