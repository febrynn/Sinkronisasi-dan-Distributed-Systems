import asyncio
from src.consensus.raft import RaftNode

async def main():
    # Buat 3 node Raft
    node1 = RaftNode("node1", 5001, [("localhost", 5002), ("localhost", 5003)])
    node2 = RaftNode("node2", 5002, [("localhost", 5001), ("localhost", 5003)])
    node3 = RaftNode("node3", 5003, [("localhost", 5001), ("localhost", 5002)])

    # Jalankan node secara bersamaan
    tasks = [
        asyncio.create_task(node1.run()),
        asyncio.create_task(node2.run()),
        asyncio.create_task(node3.run())
    ]

    # Tunggu beberapa detik sampai leader terpilih
    await asyncio.sleep(6)

    # Simulasi client meminta lock ke leader
    print("\n=== CLIENT REQUEST ===")
    await node1.append_entry({
        "action": "acquire",
        "resource": "fileA",
        "client": "client1",
        "type": "exclusive"
    })

    await asyncio.sleep(2)

    await node1.append_entry({
        "action": "release",
        "resource": "fileA",
        "client": "client1"
    })

    # Tunggu sinkronisasi selesai
    await asyncio.sleep(2)

    # Cek status akhir setiap node
    print("\n=== STATUS AKHIR ===")
    for i, node in enumerate([node1, node2, node3], start=1):
        print(f"Node{i}:", node.lock_manager.get_status())

    # Cancel semua task Raft agar program selesai
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
    asyncio.run(main())
