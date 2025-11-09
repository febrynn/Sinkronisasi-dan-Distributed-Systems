import requests
import time
import statistics
import concurrent.futures
import json
import os

# =========================================================
# === KONFIGURASI NODE ====================================
# =========================================================
BASE_URLS = [
    "http://localhost:6001",
    "http://localhost:6002",
    "http://localhost:6003"
]

results = {}

# =========================================================
# === TEST 1: LOCK MANAGER ================================
# =========================================================
def test_lock():
    latencies = []
    for i in range(50):
        node = BASE_URLS[i % len(BASE_URLS)]
        data = {"resource": f"res-{i}", "client_id": f"cli-{i}", "lock_type": "exclusive"}
        start = time.time()
        try:
            requests.post(f"{node}/lock", json=data, timeout=3)
            requests.post(f"{node}/release", json=data, timeout=3)
        except Exception as e:
            print(f"[LOCK] Error on {node}: {e}")
        latencies.append(time.time() - start)

    avg_lock = statistics.mean(latencies) * 1000
    print(f"🔐 [LOCK] Average lock acquisition: {avg_lock:.2f} ms (50 ops)")
    results["lock"] = {
        "average_lock_ms": round(avg_lock, 2),
        "operations": len(latencies)
    }

# =========================================================
# === TEST 2: QUEUE SYSTEM ================================
# =========================================================
def enqueue_message(node, i):
    data = {"topic": "perf-test", "message": {"id": i, "content": f"msg-{i}"}}
    start = time.time()
    try:
        requests.post(f"{node}/queue/enqueue", json=data, timeout=3)
    except Exception:
        pass
    return time.time() - start

def dequeue_message(node):
    start = time.time()
    try:
        requests.get(f"{node}/queue/dequeue?topic=perf-test", timeout=3)
    except Exception:
        pass
    return time.time() - start

def test_queue():
    print("🚀 Testing queue throughput...")
    enqueue_lat = []
    dequeue_lat = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
        futures = [ex.submit(enqueue_message, BASE_URLS[i % 3], i) for i in range(200)]
        for f in concurrent.futures.as_completed(futures):
            enqueue_lat.append(f.result())
        futures = [ex.submit(dequeue_message, BASE_URLS[i % 3]) for i in range(200)]
        for f in concurrent.futures.as_completed(futures):
            dequeue_lat.append(f.result())

    avg_enqueue = statistics.mean(enqueue_lat) * 1000
    avg_dequeue = statistics.mean(dequeue_lat) * 1000
    throughput = len(enqueue_lat) / sum(enqueue_lat)
    print(f"📦 [QUEUE] Avg enqueue: {avg_enqueue:.2f} ms, Avg dequeue: {avg_dequeue:.2f} ms, Throughput: {throughput:.1f} msg/sec")
    results["queue"] = {
        "avg_enqueue_ms": round(avg_enqueue, 2),
        "avg_dequeue_ms": round(avg_dequeue, 2),
        "throughput_msg_per_sec": round(throughput, 2),
        "messages_tested": len(enqueue_lat)
    }

# =========================================================
# === TEST 3: CACHE SYSTEM ================================
# =========================================================
def test_cache():
    print("🧠 Testing cache performance...")
    keys = [f"key-{i}" for i in range(200)]
    hits, misses = 0, 0
    start = time.time()

    # Set data ke cache
    for i, key in enumerate(keys):
        node = BASE_URLS[i % 3]
        data = {"key": key, "value": {"data": f"value-{i}"}}
        try:
            requests.post(f"{node}/cache/set", json=data, timeout=3)
        except Exception:
            pass

    # Baca dua kali (untuk hit/miss)
    for i, key in enumerate(keys):
        node = BASE_URLS[i % 3]
        try:
            r1 = requests.get(f"{node}/cache/get/{key}", timeout=3)
            if r1.status_code == 200:
                hits += 1
            r2 = requests.get(f"{node}/cache/get/{key}", timeout=3)
            if r2.status_code == 200:
                hits += 1
            else:
                misses += 1
        except Exception:
            misses += 1

    total_ops = hits + misses
    hit_rate = (hits / total_ops) * 100 if total_ops else 0
    duration = time.time() - start
    throughput = total_ops / duration
    print(f"💾 [CACHE] Hit rate: {hit_rate:.1f}%, Throughput: {throughput:.1f} ops/sec, Duration: {duration:.2f}s")
    results["cache"] = {
        "hit_rate_percent": round(hit_rate, 2),
        "throughput_ops_per_sec": round(throughput, 2),
        "duration_sec": round(duration, 2),
        "total_operations": total_ops
    }

# =========================================================
# === TEST 4: SCALABILITY =================================
# =========================================================
def test_scalability():
    print("\n📈 Testing scalability across node counts...")
    scalability_results = {}

    for n_nodes in [1, 2, 3]:
        nodes = BASE_URLS[:n_nodes]
        start = time.time()
        total_reqs = 0

        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
            futures = []
            for i in range(300):
                node = nodes[i % n_nodes]
                data = {"key": f"scale-{i}", "value": {"data": f"value-{i}"}}
                futures.append(ex.submit(requests.post, f"{node}/cache/set", json=data, timeout=3))
            for f in concurrent.futures.as_completed(futures):
                total_reqs += 1

        duration = time.time() - start
        throughput = total_reqs / duration
        scalability_results[n_nodes] = round(throughput, 2)
        print(f"⚙️  {n_nodes} Node(s): Throughput {throughput:.2f} req/sec")

    results["scalability"] = scalability_results

# =========================================================
# === MAIN BENCHMARK ======================================
# =========================================================
if __name__ == "__main__":
    print("=== FULL SYSTEM BENCHMARK START ===")
    print("[TEST 1] LOCK MANAGER")
    test_lock()
    print("\n[TEST 2] QUEUE SYSTEM")
    test_queue()
    print("\n[TEST 3] CACHE SYSTEM")
    test_cache()
    print("\n[TEST 4] SCALABILITY TEST")
    test_scalability()
    print("\n=== BENCHMARK COMPLETE ===")

    # Simpan hasil ke file JSON
    os.makedirs("benchmarks", exist_ok=True)
    with open("benchmarks/results.json", "w") as f:
        json.dump(results, f, indent=4)

    print("\n✅ Hasil disimpan ke benchmarks/results.json")
