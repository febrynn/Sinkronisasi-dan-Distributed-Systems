import requests
import json
import time

# Konfigurasi node (ports yang terekspos)
NODE_URLS = {
    "node_1": "http://localhost:5001",
    "node_2": "http://localhost:5002",
    "node_3": "http://localhost:5003",
}

def send_request(node_id, endpoint, data=None):
    """Mengirim permintaan POST ke node tertentu."""
    url = f"{NODE_URLS[node_id]}/{endpoint}"
    try:
        if data:
            response = requests.post(url, json=data, timeout=5)
        else:
            response = requests.get(url, timeout=5)
        
        # Cek apakah ada pengalihan (redirect) yang disarankan oleh Raft Follower
        if 'is_redirect' in response.json() and response.json().get('is_redirect'):
            leader_id = response.json().get('leader_id')
            print(f"[{node_id}] 🔄 Redirect ke Leader: {leader_id}")
            if leader_id in NODE_URLS:
                # Coba kirim lagi ke Leader yang disarankan
                return send_request(leader_id, endpoint, data)
            else:
                return response.json()
        
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        return {"error": f"Koneksi gagal atau error HTTP: {e}"}

def test_distributed_lock_manager():
    print("--- Memulai Pengujian DLM ---")

    # 1. Cek Status Awal Node 1
    status_node1 = send_request("node_1", "status")
    print(f"\n[NODE 1 Status Awal]:\n{json.dumps(status_node1, indent=2)}")

    # Cari Leader (asumsi Leader terpilih setelah beberapa saat)
    leader_id = None
    for i in range(1, 4):
        node_id = f"node_{i}"
        status = send_request(node_id, "status")
        if status.get("role") == "leader":
            leader_id = node_id
            break
        time.sleep(1) 

    if not leader_id:
        print("\n❌ GAGAL menemukan Leader. Pastikan semua node Raft berjalan.")
        return

    print(f"\n🎉 Leader Terpilih: {leader_id}")
    
    # 2. Klien A meminta kunci EXCLUSIVE
    print("\n--- TEST 1: Acquire Exclusive Lock (R1 oleh Klien A) ---")
    req_1 = {"action": "acquire_lock", "resource": "R1", "client": "KlienA", "lock_type": "exclusive"}
    res_1 = send_request("node_1", "request", req_1) 
    print(f"Hasil Request 1:\n{json.dumps(res_1, indent=2)}")

    # 3. Klien B meminta kunci yang sama (HARUS DITOLAK/WAITING)
    print("\n--- TEST 2: Acquire Exclusive Lock (R1 oleh Klien B, harus Waiting) ---")
    req_2 = {"action": "acquire_lock", "resource": "R1", "client": "KlienB", "lock_type": "exclusive"}
    res_2 = send_request(leader_id, "request", req_2) 
    print(f"Hasil Request 2:\n{json.dumps(res_2, indent=2)}")

    # 4. Cek Status Lock R1 (harus menunjukkan KlienA sebagai holder, KlienB sebagai waiting)
    print("\n--- TEST 3: Verifikasi Status Lock R1 ---")
    status_r1 = send_request(leader_id, "status?resource=R1")
    print(f"Status R1:\n{json.dumps(status_r1, indent=2)}")
    
    # 5. Klien A melepaskan kunci (HARUS MEMICU KLIEN B UNTUK MENDAPATKAN KUNCI)
    print("\n--- TEST 4: Release Lock (R1 oleh Klien A) ---")
    req_3 = {"action": "release_lock", "resource": "R1", "client": "KlienA"}
    res_3 = send_request(leader_id, "request", req_3)
    print(f"Hasil Request 3:\n{json.dumps(res_3, indent=2)}")
    
    # 6. Cek Status Lock R1 (sekarang harus menunjukkan KlienB sebagai holder)
    print("\n--- TEST 5: Verifikasi Status Lock R1 Setelah Release ---")
    # Beri waktu sebentar agar operasi release log teraplikasi di seluruh node
    time.sleep(1) 
    status_r1_final = send_request(leader_id, "status?resource=R1")
    print(f"Status R1 Akhir:\n{json.dumps(status_r1_final, indent=2)}")

    print("\n--- Pengujian Selesai ---")

if __name__ == "__main__":
    test_distributed_lock_manager()
