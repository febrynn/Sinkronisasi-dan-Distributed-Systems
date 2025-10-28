import sys
import os
import asyncio
import threading
from flask import Flask, request, jsonify
import time
import logging

# Pastikan path modul src bisa ditemukan
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from src.consensus.raft import RaftNode  # Pastikan file ini ada

app = Flask(__name__)
raft_node = None
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
    for attempt in range(3):
        try:
            future = asyncio.run_coroutine_threadsafe(coro, raft_loop)
            return future.result(timeout=5)
        except asyncio.TimeoutError:
            print(f"[{raft_node.node_id}] WARNING: Timeout saat menjalankan async_to_sync (percobaan {attempt+1})")
            time.sleep(0.2)
        except Exception as e:
            print(f"[{raft_node.node_id}] ERROR di async_to_sync: {e}")
            time.sleep(0.2)
    return None

# =========================================================
# === BAGIAN: RPC ENDPOINT UNTUK ANTAR-NODE ===============
# =========================================================
@app.route("/message", methods=["POST"])
def rpc_message():
    """Endpoint untuk menerima pesan RPC antar node Raft."""
    global raft_node
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No JSON payload provided"}), 400

        response_data = async_to_sync(raft_node.handle_message_rpc(data))
        if not response_data:
            response_data = {"status": "received"}

        return jsonify(response_data), 200

    except Exception as e:
        print(f"[{raft_node.node_id}] RPC Error: {e}")
        return jsonify({"error": str(e)}), 500


# =========================================================
# === BAGIAN: CLIENT API UNTUK USER EKSTERNAL =============
# =========================================================
@app.route("/request", methods=["POST"])
def client_request():
    """Menangani permintaan klien (acquire/release lock)."""
    global raft_node

    if raft_node.role != "leader":
        leader_host, leader_port = raft_node.get_leader_address()
        if leader_host and leader_port:
            return jsonify({
                "is_redirect": True,
                "leader_id": raft_node.leader_id,
                "leader_address": f"{leader_host}:{leader_port}",
                "message": "Redirecting to leader"
            }), 307
        else:
            return jsonify({
                "error": "No leader available",
                "is_redirect": True,
                "leader_id": None
            }), 503

    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No JSON payload provided"}), 400

        result = async_to_sync(raft_node.handle_client_request(data))
        return jsonify({"status": "success", "result": result}), 200

    except Exception as e:
        return jsonify({"error": f"Error processing client request: {str(e)}"}), 500


@app.route("/status", methods=["GET"])
def status():
    """Melihat status node saat ini (role, term, leader, dsb)."""
    global raft_node
    try:
        result = async_to_sync(raft_node.get_node_status())
        if not result:
            raise Exception("Empty status returned")
        return jsonify(result)
    except Exception as e:
        logging.exception(f"[ERROR /status] {e}")
        return jsonify({"error": f"Could not retrieve status: {str(e)}"}), 500


# =========================================================
# === ENTRY POINT =========================================
# =========================================================
def main():
    """Entry point utama untuk menjalankan Flask + Raft Node."""
    global raft_node, raft_loop

    if len(sys.argv) < 4:
        print("Usage: python -m src.app <node_id> <port> [peer_host:peer_port...]")
        sys.exit(1)

    node_id = sys.argv[1]
    port = int(sys.argv[2])

    # Kumpulkan daftar peer nodes
    peers = {}
    for p in sys.argv[3:]:
        try:
            host, port_str = p.split(":")
            peer_port = int(port_str)
            peers[host] = (host, peer_port)
        except ValueError:
            print(f"Invalid peer format: {p}. Use format 'host:port'.")
            sys.exit(1)

    # Inisialisasi node Raft
    raft_node = RaftNode(node_id, port, peers)
    print(f"[{raft_node.node_id}] RaftNode initialized successfully")


    # Jalankan loop Raft di thread terpisah
    raft_thread = threading.Thread(target=run_raft_core, args=(raft_loop, raft_node), daemon=True)
    raft_thread.start()

    # Jalankan Flask di main thread
    print(f"[{raft_node.node_id}] Flask Server running on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()
