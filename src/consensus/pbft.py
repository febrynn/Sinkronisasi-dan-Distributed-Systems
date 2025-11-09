import asyncio
from typing import Dict, List

class PBFTNode:
    def __init__(self, node_id: str, peers: List[str]):
        self.node_id = node_id
        self.peers = peers 
        self.state = "normal"
        self.current_view = 0
        self.message_log = []          
        self.committed_requests = []   

    async def handle_request(self, request: Dict):
        print(f"[{self.node_id}] Menerima request: {request}")
        await self.pre_prepare(request)

    async def pre_prepare(self, request: Dict):
        msg = {
            "type": "PRE-PREPARE",
            "view": self.current_view,
            "request": request,
            "sender": self.node_id
        }
        print(f"[{self.node_id}] PRE-PREPARE {request}")
        await self.broadcast(msg)
        await self.prepare(msg)

    async def prepare(self, msg: Dict):
        digest = hash(str(msg["request"]))
        msg2 = {
            "type": "PREPARE",
            "view": msg["view"],
            "digest": digest,
            "sender": self.node_id
        }
        print(f"[{self.node_id}] PREPARE {digest}")
        await self.broadcast(msg2)
        await self.commit(msg2)

    async def commit(self, msg: Dict):
        msg3 = {
            "type": "COMMIT",
            "view": msg["view"],
            "digest": msg["digest"],
            "sender": self.node_id
        }
        self.message_log.append(msg3)
        await self.broadcast(msg3)
        self.committed_requests.append(msg.get("request"))
        print(f"[{self.node_id}] ✅ Commit selesai untuk {msg3['digest']}")

    async def broadcast(self, msg: Dict):
        print(f"[{self.node_id}] Broadcast: {msg['type']} to {len(self.peers)} peers.")
        # Simulasikan pengiriman pesan ke peers.
        # Dalam implementasi nyata, ini akan memanggil send_message BaseNode
    
