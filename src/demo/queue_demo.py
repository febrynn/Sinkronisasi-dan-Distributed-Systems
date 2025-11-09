# src/demo/queue_demo.py

import sys
import os
import time
import argparse
import random

# Import Absolut yang Benar
from nodes.queue_node import QueueNode 


def run_producer(node_id, topic, num_messages, rate):
    node = QueueNode(node_id)
    print(f"[Producer-{node_id}] Sending {num_messages} messages at {rate} msg/s...")
    for i in range(num_messages):
        client_id = f"client-{random.randint(1000, 9999)}" 
        message_content = {"id": i+1, "client_id": client_id, "content": f"Message {i+1} from {node_id}"}
        
        node.enqueue(topic, message_content)
        
        # BARIS INI KINI BERSIH DARI U+00A0
        print(f"[Producer-{node_id}] Sent: {message_content['content']}")
        time.sleep(1 / rate)  # rate = messages per second


# --- FUNGSI CONSUMER DENGAN LOGIKA TANPA BATAS/TERBATAS ---

def run_consumer(node_id, topic, poll_interval=1, expected_messages=None):
    node = QueueNode(node_id)
    print(f"[Consumer-{node_id}] Listening on topic '{topic}'...")
    
    messages_received = 0 
    
    # KONDISI WHILE: Loop berjalan terus JIKA expected_messages adalah None (tanpa batas)
    # ATAU jika jumlah pesan yang diterima belum mencapai target.
    while expected_messages is None or messages_received < expected_messages:
        
        msg = node.dequeue(topic, timeout=poll_interval)
        
        if msg:
            messages_received += 1 
            print(f"[Consumer-{node_id}] Received: {msg}")

            # KONDISI BREAK: Hanya terjadi jika target BUKAN None
            if expected_messages is not None and messages_received >= expected_messages:
                print(f"[Consumer-{node_id}] All {expected_messages} messages processed. Stopping.")
                break 
                
        else:
            # Jika queue kosong, istirahat sejenak
            time.sleep(poll_interval)


def main():
    parser = argparse.ArgumentParser(description="Queue Demo: Producer/Consumer")
    parser.add_argument("--role", choices=["producer", "consumer"], required=True)
    parser.add_argument("--node_id", default="node_demo")
    parser.add_argument("--topic", default="test-queue")
    
    # Kita pertahankan --messages untuk mode terbatas
    parser.add_argument("--messages", type=int, default=10, 
                        help="Jumlah message yang dikirim (Producer) atau diproses (Consumer mode terbatas).")
    
    # Flag Boolean untuk mode tak terbatas
    parser.add_argument("--infinite", action="store_true", default=False,
                        help="Jika diset, Consumer akan berjalan tanpa batas waktu.")

    parser.add_argument("--rate", type=float, default=1, help="Rate pesan per detik (producer)")
    parser.add_argument("--poll", type=float, default=1, help="Polling interval detik (consumer)")
    args = parser.parse_args()

    if args.role == "producer":
        run_producer(args.node_id, args.topic, args.messages, args.rate)
        
    elif args.role == "consumer":
        # Logika Penentuan Target Pesan untuk Consumer
        if args.infinite:
            # Mode Tanpa Batas: target_messages = None
            target_messages = None
            print(f"[{args.node_id}] Starting consumer in INFINITE mode. Press CTRL+C to stop.")
        else:
            # Mode Terbatas: target_messages = args.messages (default 10)
            target_messages = args.messages
            
        run_consumer(args.node_id, args.topic, args.poll, expected_messages=target_messages)

if __name__ == "__main__":
    main()