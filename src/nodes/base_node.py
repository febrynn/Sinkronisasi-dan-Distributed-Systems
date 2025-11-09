import asyncio
import sys
import os
import aiohttp
from aiohttp import web
import json
import redis.asyncio as redis
from abc import ABC, abstractmethod

class BaseNode(ABC):

    def __init__(self, node_id: str, port: int, peers: dict):
        self.node_id = node_id
        self.port = port
        # Peers adalah dictionary: { 'node_id': ('host', port), ... }
        self.peers = peers 
        self.app = web.Application()
        # Semua komunikasi antar node (RPC) akan masuk ke endpoint '/message'
        self.app.add_routes([web.post('/message', self.handle_rpc_request)])

        # Ambil konfigurasi Redis dari environment variables (disediakan Docker)
        redis_host = os.getenv('REDIS_HOST', 'localhost')
        redis_port = int(os.getenv('REDIS_PORT', 6379))
        self.redis_client = redis.Redis(host=redis_host, port=redis_port)
        print(f"[{self.node_id}] Terhubung ke Redis di {redis_host}:{redis_port}")


    async def handle_rpc_request(self, request):
        """Menerima dan memproses semua permintaan RPC dari node lain."""
        try:
            data = await request.json()
            # Panggil method handle_message_rpc yang HARUS diimplementasikan di subclass
            response_data = await self.handle_message_rpc(data)
            
            if isinstance(response_data, dict):
                return web.json_response({"status": "OK", "data": response_data})
            elif isinstance(response_data, web.Response):
                return response_data
            else:
                return web.json_response({"status": "OK", "message": "Message processed"})

        except Exception as e:
            print(f"[{self.node_id}] ERROR memproses RPC dari {request.remote}: {e}")
            return web.json_response({"status": "ERROR", "message": str(e)}, status=500)
    
    @abstractmethod
    async def handle_message_rpc(self, data: dict):
        """
        METHOD ABSTRAK: Harus diimplementasikan oleh subclass (RaftNode, PBFTNode, dll.).
        Bertanggung jawab untuk memproses pesan RPC spesifik (RequestVote, AppendEntries, dll.).
        """
        pass
    
    async def send_rpc(self, target_key: str, endpoint: str, data: dict):
        """Mengirimkan RPC (Remote Procedure Call) ke node peer."""
        host, port = self.peers.get(target_key, (None, None))
        if not host or not port:
            return {"error": f"Peer {target_key} tidak ditemukan"}

        url = f"http://{host}:{port}{endpoint}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=data, timeout=aiohttp.ClientTimeout(total=2.0)) as response:
                    result = await response.json()
                    # Return hanya data, bukan tuple
                    return result.get("data", result)
        except aiohttp.ClientConnectorError as e:
            return {"error": f"Koneksi gagal ke {target_key} di {host}:{port}"}
        except asyncio.TimeoutError:
            return {"error": f"Timeout connecting to {target_key}"}
        except Exception as e:
            return {"error": f"Gagal mengirim RPC ke {target_key}: {str(e)}"}

    async def broadcast_rpc(self, endpoint: str, data: dict, exclude_self=True):
        """Mengirimkan RPC ke semua peer secara bersamaan."""
        tasks = []
        for peer_id in self.peers:
            if exclude_self and peer_id == self.node_id:
                continue
            
            task = self.send_rpc(peer_id, endpoint, data)
            tasks.append(task)

        return await asyncio.gather(*tasks, return_exceptions=True)

    async def start_server(self):
        """Memulai HTTP Server (AIOHTTP) untuk menerima RPC."""
        try:
            runner = web.AppRunner(self.app)
            await runner.setup()
            site = web.TCPSite(runner, '0.0.0.0', self.port) 
            await site.start()
            print(f"[{self.node_id}] Server jalan di port {self.port} (0.0.0.0)")
        except Exception as e:
            print(f"[{self.node_id}] ERROR saat memulai server: {e}")
            sys.exit(1)

    @abstractmethod
    async def run(self):
        """
        METHOD ABSTRAK: Main loop node (Raft logic), harus diimplementasikan di subclass.
        """
        pass

# Jika dieksekusi langsung
if __name__ == "__main__":
    print("BaseNode tidak bisa dijalankan langsung karena merupakan kelas abstrak.")
    sys.exit(1)