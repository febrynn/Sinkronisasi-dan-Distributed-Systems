# import aiohttp
# import asyncio
# import sys
# import json

# LEADER_PORT = 5001  # Port node Raft yang diasumsikan Leader atau target pertama
# LEADER_HOST = 'localhost' # Ganti jika dijalankan di Docker/remote

# async def send_command(host: str, port: int, action: str, resource: str, client: str, lock_type: str = "exclusive"):
#     """
#     Mengirim perintah (command) ke node Raft.
#     Node Raft akan meneruskan command ini ke Leader, yang kemudian akan
#     menerapkannya melalui replikasi log.
#     """
#     url = f"http://{host}:{port}/message"
    
#     # Payload yang dikirim ke RaftNode
#     # RaftNode akan mengekstrak command ini dan memasukkannya ke log.
#     command_payload = {
#         "action": action,
#         "resource": resource,
#         "client": client,
#         "type": lock_type
#     }
    
#     rpc_payload = {
#         "sender": "ClientSimulator",
#         # Di RaftNode, tipe "client_command" akan memanggil method append_entry
#         "type": "client_command", 
#         "content": command_payload 
#     }

#     print(f"\n[CLIENT] Mengirim '{action}' untuk '{resource}' ({lock_type}) oleh '{client}' ke {host}:{port}")

#     try:
#         async with aiohttp.ClientSession() as session:
#             async with session.post(url, json=rpc_payload, timeout=5.0) as resp:
#                 response = await resp.json()
                
#                 # Cek jika Raft Node me-redirect ke Leader
#                 if not response.get('success') and response.get('redirect_to'):
#                     leader_id = response['redirect_to']
#                     print(f"[CLIENT] ⚠️ Redirect: Node bukan Leader. Coba kirim ke Leader ID: {leader_id}")
#                     # Asumsi port Leader sama dengan port yang telah didefinisikan 
#                     # Jika menggunakan sistem resolusi nama, ini harus disesuaikan
#                     # Untuk simulasi lokal, kita asumsikan port tetap.
#                     if leader_id in node_ports:
#                          return await send_command(LEADER_HOST, node_ports[leader_id], action, resource, client, lock_type)
#                     else:
#                          print(f"[CLIENT] ERROR: Leader ID {leader_id} tidak dikenal.")
#                          return response
                
#                 print(f"[CLIENT] ✅ Response dari Raft: {response}")
#                 return response

#     except aiohttp.ClientConnectorError:
#         print(f"[CLIENT] ❌ Koneksi gagal ke {host}:{port}. Pastikan node Raft berjalan.")
#     except asyncio.TimeoutError:
#         print(f"[CLIENT] ⏳ Permintaan ke {host}:{port} timeout.")
#     except Exception as e:
#         print(f"[CLIENT] 🛑 Error tak terduga: {e}")
        
#     return {"success": False, "message": "Client connection failed or timed out."}

# # Mapping port untuk simulasi, sesuaikan dengan cara Anda menjalankan node Raft
# node_ports = {
#     'node1': 5001,
#     'node2': 5002,
#     'node3': 5003,
# }

# async def main():
#     if len(sys.argv) < 2:
#         print("Usage: python src/client.py <test_case_name>")
#         print("Available test cases: exclusive_queue, shared_access, upgrade_fail")
#         sys.exit(1)

#     test_case = sys.argv[1].lower()
    
#     # Target node: Kita akan selalu mengirim ke node1 (port 5001) dan biarkan Raft 
#     # menangani redirect jika node1 bukan Leader.
#     target_port = node_ports['node1']
    
#     if test_case == "exclusive_queue":
#         print("\n=============================================")
#         print("TEST CASE: EXCLUSIVE LOCK & QUEUE (FIFO)")
#         print("=============================================")
        
#         await send_command(LEADER_HOST, target_port, "acquire", "file_db", "client_A", "exclusive")
#         await send_command(LEADER_HOST, target_port, "acquire", "file_db", "client_B", "exclusive") # Harus menunggu
#         await send_command(LEADER_HOST, target_port, "acquire", "file_db", "client_C", "exclusive") # Harus menunggu
        
#         print("\n--- Status Antrian (Harusnya B & C menunggu) ---")
#         await send_command(LEADER_HOST, target_port, "query", "file_db", "client_A")

#         print("\n--- client_A Melepas Lock ---")
#         await send_command(LEADER_HOST, target_port, "release", "file_db", "client_A") # B harus mendapatkan lock

#         print("\n--- Status Setelah Release (Harusnya B jadi Holder) ---")
#         await send_command(LEADER_HOST, target_port, "query", "file_db", "client_B")
        
#         print("\n--- client_B Melepas Lock (C harus mendapatkan lock) ---")
#         await send_command(LEADER_HOST, target_port, "release", "file_db", "client_B")

#         print("\n--- Status Setelah Release (Harusnya C jadi Holder) ---")
#         await send_command(LEADER_HOST, target_port, "query", "file_db", "client_C")
        
#         await send_command(LEADER_HOST, target_port, "release", "file_db", "client_C")


#     elif test_case == "shared_access":
#         print("\n=============================================")
#         print("TEST CASE: SHARED LOCK ACCESS & RELEASE")
#         print("=============================================")
        
#         # 1. Acquire shared lock
#         await send_command(LEADER_HOST, target_port, "acquire", "cache_data", "client_X", "shared")
#         await send_command(LEADER_HOST, target_port, "acquire", "cache_data", "client_Y", "shared")
        
#         print("\n--- Status Shared (Harusnya X & Y jadi Holder) ---")
#         await send_command(LEADER_HOST, target_port, "query", "cache_data", "client_X")
        
#         # 2. Release partial
#         await send_command(LEADER_HOST, target_port, "release", "cache_data", "client_X")
#         print("\n--- Status Setelah X Release (Y masih pegang) ---")
#         await send_command(LEADER_HOST, target_port, "query", "cache_data", "client_Y")

#         # 3. Request exclusive (harus menunggu)
#         await send_command(LEADER_HOST, target_port, "acquire", "cache_data", "client_Z", "exclusive")
        
#         # 4. Y release (Z harus dapat lock exclusive)
#         print("\n--- Y Melepas Lock Shared (Z harus mendapatkan Exclusive) ---")
#         await send_command(LEADER_HOST, target_port, "release", "cache_data", "client_Y")
        
#         print("\n--- Status Akhir ---")
#         await send_command(LEADER_HOST, target_port, "query", "cache_data", "client_Z")
#         await send_command(LEADER_HOST, target_port, "release", "cache_data", "client_Z")


#     else:
#         print(f"Test case '{test_case}' tidak ditemukan.")

# if __name__ == "__main__":
#     asyncio.run(main())
