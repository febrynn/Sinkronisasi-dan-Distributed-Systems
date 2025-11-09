import sys
import os
import asyncio
import threading
from flask import Flask, request, jsonify
import time
import logging
import json
import redis

CLIENT_TIMEOUT = 5.0 

# Tambahkan path agar modul src bisa diimpor
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from src.consensus.raft import RaftNode
from src.nodes.queue_node import QueueNode
from src.nodes.cache_node import CacheNode

app = Flask(__name__)
raft_node = None
queue_node = None
cache_node = None
raft_loop = asyncio.new_event_loop()

# =========================================================
# === BAGIAN: UTILITAS LOOP ASYNC RAFT ====================
# =========================================================
def run_raft_core(loop, node):
    """Menjalankan event loop Raft di thread terpisah."""
    print(f"[{node.node_id}] Starting Raft core loop in a new thread...")
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(node.run())
    except Exception as e:
        print(f"[{node.node_id}] Raft Core crashed: {e}")
    finally:
        print(f"[{node.node_id}] Raft Core loop closed.")


def async_to_sync(coro):
    """Menjalankan coroutine async di thread Flask (sinkron)."""
    global raft_loop
    for attempt in range(5): 
        try:
            future = asyncio.run_coroutine_threadsafe(coro, raft_loop)
            return future.result(CLIENT_TIMEOUT) 
        except asyncio.TimeoutError:
            print(f"[ERROR async_to_sync] Timeout after {CLIENT_TIMEOUT}s on attempt {attempt+1}")
            if attempt == 4:
                raise TimeoutError("Async operation timed out")
            time.sleep(0.1)
        except Exception as e:
            print(f"[ERROR async_to_sync] Gagal di percobaan {attempt+1}: {e}")
            if attempt == 4:
                raise e
            time.sleep(0.1)


# =========================================================
# === BAGIAN: LOCK ENDPOINTS ==============================
# =========================================================

@app.route('/lock', methods=['POST'])
def lock_resource():
    """Endpoint untuk request lock dari klien."""
    if not raft_node:
        return jsonify({"error": "Raft node not initialized."}), 503
    
    data = request.get_json()
    resource = data.get('resource')
    client_id = data.get('client_id')
    lock_type = data.get('lock_type', 'exclusive')

    if not all([resource, client_id]):
        return jsonify({"error": "Missing 'resource' or 'client_id' in request."}), 400

    # Saat ini, LockManager dijalankan langsung. 
    # TODO: Di masa depan, command ini HARUS direplikasi melalui Raft.
    try:
        # Perintah ini TIDAK akan direplikasi oleh Raft, ini hanya state lokal
        # yang akan di-reset jika node restart tanpa persistensi.
        result = raft_node.lock_manager.acquire_lock(resource, client_id, lock_type)
        
        # NOTE: Agar konsisten, kita seharusnya menggunakan Raft:
        # command = json.dumps({"op": "acquire", "resource": resource, "client_id": client_id, "type": lock_type})
        # response = async_to_sync(raft_node.handle_client_command(command))
        
        return jsonify({
            "status": "ok",
            "message": result
        }), 200
    except Exception as e:
        print(f"[ERROR /lock] {e}")
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500



@app.route('/release', methods=['POST'])
def release_resource():
    """Endpoint untuk melepaskan lock dari klien. HARUS melalui Raft (Leader)."""
    if not raft_node:
        return jsonify({"error": "Raft node not initialized."}), 503
    
    data = request.get_json()
    resource = data.get('resource')
    client_id = data.get('client_id')

    if not all([resource, client_id]):
        return jsonify({"error": "Missing 'resource' or 'client_id' in request."}), 400

    # Perintah release harus melalui Raft untuk konsistensi
    command = json.dumps({
        "op": "release",
        "resource": resource,
        "client_id": client_id
    })
    
    try:
        response = async_to_sync(raft_node.handle_client_command(command))
        
        if response.get("is_redirect"):
            return jsonify({
                "status": "redirect",
                "message": "Bukan leader, redirect ke leader.",
                "leader_id": response["leader_id"],
                "leader_address": response["leader_address"]
            }), 307
            
        return jsonify(response), 200

    except TimeoutError:
        return jsonify({"error": "Request timed out waiting for Raft replication."}), 504
    except Exception as e:
        print(f"[ERROR /release] {e}")
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500

@app.route('/lock/status', methods=['GET'])
def lock_status():
    """Cek status semua resource atau 1 resource tertentu"""
    if not raft_node:
        return jsonify({"error": "Raft node not initialized."}), 503

    resource = request.args.get("resource")

    try:
        # Ambil status langsung dari state machine (LockManager)
        # LockManager diakses melalui raft_node.state_machine yang merupakan LockManager itu sendiri
        status_json = raft_node.lock_manager.get_status(resource)
        status = json.loads(status_json)  # ubah dari string ke dict
        return jsonify(status)
    except Exception as e:
        print(f"[ERROR /lock/status] {e}")
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500

# =========================================================
# === BAGIAN: QUEUE ENDPOINTS =============================
# =========================================================

@app.route('/queue/enqueue', methods=['POST'])
def enqueue_message():
    """Enqueue message ke distributed queue."""
    if not queue_node:
        return jsonify({"error": "Queue node not initialized."}), 503
    
    try:
        data = request.get_json()
        topic = data.get('topic', 'default-queue')
        message = data.get('message')
        
        if not message:
            return jsonify({"error": "Missing 'message' in request."}), 400
        
        result = queue_node.enqueue(topic, message)
        return jsonify(result), 200
        
    except Exception as e:
        print(f"[ERROR /queue/enqueue] {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/queue/dequeue', methods=['GET'])
def dequeue_message():
    """Dequeue message dari distributed queue."""
    if not queue_node:
        return jsonify({"error": "Queue node not initialized."}), 503
    
    try:
        topic = request.args.get('topic', 'default-queue')
        timeout = int(request.args.get('timeout', 5))
        
        message = queue_node.dequeue(topic, timeout)
        
        if message:
            return jsonify({"status": "success", "message": message}), 200
        else:
            return jsonify({"status": "empty", "message": "No messages available"}), 200
            
    except Exception as e:
        print(f"[ERROR /queue/dequeue] {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/queue/stats', methods=['GET'])
def queue_stats():
    """Get queue statistics with throughput metrics."""
    if not queue_node:
        return jsonify({"error": "Queue node not initialized."}), 503
    
    try:
        # GUNAKAN method get_stats() yang sudah ada throughput!
        stats = queue_node.get_stats()
        return jsonify(stats), 200
        
    except Exception as e:
        print(f"[ERROR /queue/stats] {e}")
        return jsonify({"error": str(e)}), 500
# =========================================================
# === BAGIAN: CACHE ENDPOINTS =============================
# =========================================================

@app.route('/cache/get/<key>', methods=['GET'])
def cache_get(key):
    """Get value from cache."""
    if not cache_node:
        return jsonify({"error": "Cache node not initialized."}), 503
    
    try:
        value = cache_node.read(key)
        
        if value:
            return jsonify({
                "status": "hit",
                "key": key,
                "value": value,
                "node_id": cache_node.node_id
            }), 200
        else:
            return jsonify({
                "status": "miss",
                "key": key,
                "message": "Key not found"
            }), 404
            
    except Exception as e:
        print(f"[ERROR /cache/get] {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/cache/set', methods=['POST'])
def cache_set():
    """Set value in cache."""
    if not cache_node:
        return jsonify({"error": "Cache node not initialized."}), 503
    
    try:
        data = request.get_json()
        key = data.get('key')
        value = data.get('value')
        
        if not key:
            return jsonify({"error": "Missing 'key' in request."}), 400
        
        cache_node.write(key, value)
        
        return jsonify({
            "status": "success",
            "key": key,
            "message": "Value cached successfully",
            "node_id": cache_node.node_id
        }), 200
        
    except Exception as e:
        print(f"[ERROR /cache/set] {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/cache/invalidate', methods=['POST'])
def cache_invalidate():
    """Invalidate cache entry."""
    if not cache_node:
        return jsonify({"error": "Cache node not initialized."}), 503
    
    try:
        data = request.get_json()
        key = data.get('key')
        
        if not key:
            return jsonify({"error": "Missing 'key' in request."}), 400
        
        cache_node.invalidate(key)
        
        return jsonify({
            "status": "success",
            "key": key,
            "message": "Cache invalidated",
            "node_id": cache_node.node_id
        }), 200
        
    except Exception as e:
        print(f"[ERROR /cache/invalidate] {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/cache/stats', methods=['GET'])
def cache_stats():
    """Get cache statistics."""
    if not cache_node:
        return jsonify({"error": "Cache node not initialized."}), 503
    
    try:
        status = cache_node.get_status()
        
        return jsonify({
            "node_id": cache_node.node_id,
            "cache_entries": len(cache_node.cache),
            "cache_status": status,
            "timestamp": time.time()
        }), 200
        
    except Exception as e:
        print(f"[ERROR /cache/stats] {e}")
        return jsonify({"error": str(e)}), 500


# =========================================================
# === BAGIAN: STATUS & HEALTH ENDPOINTS ===================
# =========================================================

@app.route('/status', methods=['GET'])
def node_status():
    """Endpoint untuk mendapatkan status Raft node saat ini."""
    if not raft_node:
        return jsonify({"error": "Raft node not initialized."}), 503
    
    try:
        status = async_to_sync(raft_node.get_node_status())
        return jsonify(status), 200
    except Exception as e:
        print(f"[ERROR /status] {e}")
        return jsonify({"error": f"Could not retrieve status: {str(e)}"}), 500


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    try:
        health_status = {
            "status": "healthy",
            "node_id": raft_node.node_id if raft_node else "unknown",
            "components": {
                "raft": "up" if raft_node else "down",
                "queue": "up" if queue_node else "down",
                "cache": "up" if cache_node else "down"
            },
            "timestamp": time.time()
        }
        
        # Check Redis connection
        try:
            if queue_node:
                queue_node.redis.ping()
                health_status["components"]["redis"] = "up"
        except:
                health_status["components"]["redis"] = "down"
                health_status["status"] = "degraded"
        
        status_code = 200 if health_status["status"] == "healthy" else 503
        return jsonify(health_status), status_code
        
    except Exception as e:
        return jsonify({
            "status": "unhealthy",
            "error": str(e)
        }), 503


# =========================================================
# === ENTRY POINT =========================================
# =========================================================
def main():
    """Entry point utama untuk menjalankan Flask + Raft Node."""
    global raft_node, queue_node, cache_node, raft_loop

    if len(sys.argv) < 4:
        print("Usage: python -m src.app <node_id> <raft_rpc_port> <peer_id:peer_host:peer_port> ...")
        print("Example: python -m src.app node_1 5001 node_2:lock_node_2:5002 node_3:lock_node_3:5003")
        sys.exit(1)

    node_id = sys.argv[1]
    raft_rpc_port = int(sys.argv[2])
    
    # Port Flask API = port RPC + 1000
    client_api_port = raft_rpc_port + 1000

    # Parse peers dengan format: peer_id:host:port
    peers = {}
    for p in sys.argv[3:]:
        try:
            parts = p.split(":")
            if len(parts) != 3:
                print(f"Invalid peer format: {p}. Use format 'peer_id:host:port'.")
                sys.exit(1)
            
            peer_id, host, port_str = parts
            peer_port = int(port_str)
            peers[peer_id] = (host, peer_port)
            print(f"[{node_id}] Added peer: {peer_id} at {host}:{peer_port}")
        except ValueError as e:
            print(f"Error parsing peer {p}: {e}")
            sys.exit(1)

    # Inisialisasi Raft node
    raft_node = RaftNode(node_id, raft_rpc_port, peers)
    print(f"[{node_id}] RaftNode initialized successfully")

    # Inisialisasi Queue node
    redis_host = os.getenv('REDIS_HOST', 'localhost')
    redis_port = int(os.getenv('REDIS_PORT', 6379))
    
    try:
        queue_node = QueueNode(node_id, redis_host, redis_port)
        print(f"[{node_id}] QueueNode initialized successfully")
    except Exception as e:
        print(f"[{node_id}] WARNING: QueueNode initialization failed: {e}")
    
    # Inisialisasi Cache node
    try:
        cache_node = CacheNode(node_id, redis_host, redis_port)
        print(f"[{node_id}] CacheNode initialized successfully")
    except Exception as e:
        print(f"[{node_id}] WARNING: CacheNode initialization failed: {e}")

    # Jalankan loop Raft di thread terpisah
    raft_thread = threading.Thread(target=run_raft_core, args=(raft_loop, raft_node))
    raft_thread.daemon = True
    raft_thread.start()

    # Tunggu sebentar agar Raft loop siap
    time.sleep(1)

    # Jalankan server Flask untuk API klien
    print(f"[{node_id}] Flask API Server running on port {client_api_port}")
    print(f"[{node_id}] Available endpoints:")
    print(f"  - Locks: /lock, /release, /lock/status") # <--- DIPERBAIKI di sini
    print(f"  - Queue: /queue/enqueue, /queue/dequeue, /queue/stats")
    print(f"  - Cache: /cache/get/<key>, /cache/set, /cache/stats")
    print(f"  - Status: /status, /health")
    
    app.run(host='0.0.0.0', port=client_api_port, debug=False)


if __name__ == '__main__':
    try:
        logging.getLogger('werkzeug').setLevel(logging.ERROR)
        main()
    except Exception as e:
        print(f"Main application error: {e}")
        sys.exit(1)
