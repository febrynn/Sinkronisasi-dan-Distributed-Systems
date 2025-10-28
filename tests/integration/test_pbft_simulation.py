# tests/integration/test_pbft_simulation.py
import sys
import os
import pytest
import asyncio

# Tambahkan src ke path agar import src.nodes.lock_manager bisa jalan
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src")))

from nodes.lock_manager import LockManagerNode

def test_pbft_lock_simulation():
    """Simulasi integrasi 3 node PBFT LockManager"""

    async def run_simulation():
        # ==============================
        # Inisialisasi 3 node PBFT
        # ==============================
        node1 = LockManagerNode("node1", ["node2", "node3"])
        node2 = LockManagerNode("node2", ["node1", "node3"])
        node3 = LockManagerNode("node3", ["node1", "node2"])

        nodes = [node1, node2, node3]

        # ==============================
        # Simulasi client minta lock
        # ==============================
        await node1.request_lock("fileA", "client1")

        # Simulasi PBFT commit ke semua node
        for node in nodes:
            node.apply_request({
                "action": "acquire",
                "resource": "fileA",
                "client": "client1",
                "type": "exclusive"
            })

        await node1.request_lock("fileA", "client2")  # Harus nunggu
        for node in nodes:
            node.apply_request({
                "action": "acquire",
                "resource": "fileA",
                "client": "client2",
                "type": "exclusive"
            })

        # Lepas lock client1
        await node1.release_lock("fileA", "client1")
        for node in nodes:
            node.apply_request({
                "action": "release",
                "resource": "fileA",
                "client": "client1"
            })

        # Tunggu sebentar agar state sinkron
        await asyncio.sleep(0.1)

        # ==============================
        # Assert status akhir
        # ==============================
        status_list = [node.lock_manager.get_status() for node in nodes]

        # Semua node harus punya status lock yang sama
        assert status_list[0] == status_list[1] == status_list[2]

        # fileA sekarang dipegang client2
        assert status_list[0]["fileA"]["holder"] == ["client2"]

        print("\n=== STATUS AKHIR ===")
        print("Node1:", status_list[0])

    # Jalankan simulasi async di dalam pytest
    asyncio.run(run_simulation())
