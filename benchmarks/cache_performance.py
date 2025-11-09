import time
import requests
import redis

CACHE_NODE = "http://localhost:6001"

def test_with_cache(n=1000):
    print("[TEST] WITH CACHE (read only phase)")
    # Warm-up phase: tulis data dulu ke cache
    for i in range(n):
        key = f"bench:{i}"
        value = {"num": i, "double": i * 2}
        requests.post(f"{CACHE_NODE}/cache/set", json={"key": key, "value": value})

    # Benchmark read-only (cache hits)
    start = time.time()
    for i in range(n):
        key = f"bench:{i}"
        requests.get(f"{CACHE_NODE}/cache/get/{key}")
    total = time.time() - start
    print(f"Total waktu: {total:.2f} detik")
    print(f"Rata-rata latency: {total/n*1000:.2f} ms")
    print(f"Throughput: {n/total:.0f} req/sec\n")

def test_without_cache(n=1000):
    print("[TEST] WITHOUT CACHE (direct Redis)")
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    # Warm-up
    for i in range(n):
        key = f"bench:{i}"
        r.set(key, str(i))

    # Benchmark read-only
    start = time.time()
    for i in range(n):
        key = f"bench:{i}"
        r.get(key)
    total = time.time() - start
    print(f"Total waktu: {total:.2f} detik")
    print(f"Rata-rata latency: {total/n*1000:.2f} ms")
    print(f"Throughput: {n/total:.0f} req/sec\n")

if __name__ == "__main__":
    print("=== CACHE PERFORMANCE BENCHMARK (FIXED) ===\n")
    test_with_cache()
    test_without_cache()
