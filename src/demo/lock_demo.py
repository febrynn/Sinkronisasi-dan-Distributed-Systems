import requests
import time
import sys
import json

BASE_URLS = [
    "http://localhost:6001",
    "http://localhost:6002",
    "http://localhost:6003"
]

def find_leader():
    """Cari node mana yang leader"""
    for url in BASE_URLS:
        try:
            resp = requests.get(f"{url}/status", timeout=2)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("role") == "leader":
                    print(f"✓ Leader found: {data['node_id']} at {url}")
                    return url
        except:
            continue
    return BASE_URLS[0]  # fallback

def demo_basic_lock():
    """Demo 1: Basic Lock Acquisition"""
    print("\n" + "="*60)
    print("DEMO 1: Basic Lock Acquisition")
    print("="*60)
    
    leader_url = find_leader()
    
    # Client A acquire lock
    print("\n[1] Client A acquiring exclusive lock on 'database-1'...")
    resp = requests.post(f"{leader_url}/lock", json={
        "resource": "database-1",
        "client_id": "client-A",
        "lock_type": "exclusive"
    })
    print(f"Response: {resp.json()}")
    time.sleep(1)
    
    # Client B tries to acquire same lock
    print("\n[2] Client B trying to acquire same lock (should wait)...")
    resp = requests.post(f"{leader_url}/lock", json={
        "resource": "database-1",
        "client_id": "client-B",
        "lock_type": "exclusive"
    })
    print(f"Response: {resp.json()}")
    time.sleep(1)
    
    # Client A releases
    print("\n[3] Client A releasing lock...")
    resp = requests.post(f"{leader_url}/release", json={
        "resource": "database-1",
        "client_id": "client-A"
    })
    print(f"Response: {resp.json()}")

def demo_shared_locks():
    """Demo 2: Shared vs Exclusive Locks"""
    print("\n" + "="*60)
    print("DEMO 2: Shared vs Exclusive Locks")
    print("="*60)
    
    leader_url = find_leader()
    
    # Multiple clients acquire shared lock
    print("\n[1] Client A acquiring SHARED lock on 'file-1'...")
    resp = requests.post(f"{leader_url}/lock", json={
        "resource": "file-1",
        "client_id": "client-A",
        "lock_type": "shared"
    })
    print(f"Response: {resp.json()}")
    
    print("\n[2] Client B acquiring SHARED lock on 'file-1'...")
    resp = requests.post(f"{leader_url}/lock", json={
        "resource": "file-1",
        "client_id": "client-B",
        "lock_type": "shared"
    })
    print(f"Response: {resp.json()}")
    
    print("\n[3] Client C trying EXCLUSIVE lock (should wait)...")
    resp = requests.post(f"{leader_url}/lock", json={
        "resource": "file-1",
        "client_id": "client-C",
        "lock_type": "exclusive"
    })
    print(f"Response: {resp.json()}")

def demo_node_failure():
    """Demo 3: Leader Failure Recovery"""
    print("\n" + "="*60)
    print("DEMO 3: Leader Failure & Recovery")
    print("="*60)
    
    print("\n[1] Finding current leader...")
    leader_url = find_leader()
    
    print("\n[2] Acquiring lock from leader...")
    resp = requests.post(f"{leader_url}/lock", json={
        "resource": "critical-resource",
        "client_id": "client-X",
        "lock_type": "exclusive"
    })
    print(f"Response: {resp.json()}")
    
    print("\n[!] Now manually stop the leader container:")
    print("    docker-compose stop lock_node_1")
    print("\n[3] Press Enter after stopping leader...")
    input()
    
    print("\n[4] Waiting for new leader election (5 seconds)...")
    time.sleep(5)
    
    print("\n[5] Finding new leader...")
    new_leader = find_leader()
    
    print("\n[6] Trying to acquire new lock from new leader...")
    resp = requests.post(f"{new_leader}/lock", json={
        "resource": "new-resource",
        "client_id": "client-Y",
        "lock_type": "exclusive"
    })
    print(f"Response: {resp.json()}")
    print("\n✓ System recovered! Lock operations continue without downtime.")

def check_all_nodes_status():
    """Helper: Check status of all nodes"""
    print("\n" + "="*60)
    print("STATUS CHECK: All Nodes")
    print("="*60)
    
    for i, url in enumerate(BASE_URLS, 1):
        try:
            resp = requests.get(f"{url}/status", timeout=2)
            if resp.status_code == 200:
                data = resp.json()
                print(f"\nNode {i} ({url}):")
                print(f"  Role: {data['role']}")
                print(f"  Term: {data['term']}")
                print(f"  Leader: {data['leader_id']}")
                print(f"  Commit Index: {data['commit_index']}")
        except Exception as e:
            print(f"\nNode {i} ({url}): OFFLINE or ERROR - {e}")

if __name__ == "__main__":
    print("\n" + "="*60)
    print("DISTRIBUTED LOCK MANAGER DEMO")
    print("="*60)
    print("\nMake sure docker-compose is running:")
    print("  $ docker-compose up -d")
    print("\nWaiting for nodes to start (10 seconds)...")
    time.sleep(10)
    
    try:
        check_all_nodes_status()
        demo_basic_lock()
        demo_shared_locks()
        
        print("\n\nWant to demo node failure? (y/n): ", end='')
        if input().lower() == 'y':
            demo_node_failure()
        
        print("\n" + "="*60)
        print("DEMO COMPLETED!")
        print("="*60)
    except KeyboardInterrupt:
        print("\n\nDemo interrupted by user.")
    except Exception as e:
        print(f"\n\nError during demo: {e}")