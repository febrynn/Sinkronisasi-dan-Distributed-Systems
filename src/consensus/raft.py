import asyncio
import random
import time
import json
from aiohttp import web
import sys
import aiohttp
import os

# Memastikan kita dapat mengimpor dari parent
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from nodes.base_node import BaseNode
from nodes.lock_manager import LockManager

# Konstanta waktu (ms)
ELECTION_TIMEOUT_MIN = 1000
ELECTION_TIMEOUT_MAX = 2000
HEARTBEAT_INTERVAL = 300
CLIENT_TIMEOUT = 5.0

# Kunci untuk persistent state
KEY_TERM = "raft:current_term"
KEY_VOTED_FOR = "raft:voted_for"
KEY_LOG = "raft:log"


class RaftNode(BaseNode):
    """
    Implementasi inti dari algoritma Raft untuk distributed lock.
    Mengandalkan BaseNode untuk komunikasi antar-node via Redis atau RPC.
    """

    def __init__(self, node_id: str, port: int, peers: dict):
        super().__init__(node_id, port, peers)

        # Persistent state
        self.state = "follower"
        self.current_term = 0
        self.voted_for = None
        self.log = []

        # Volatile state
        self.commit_index = -1
        self.last_applied = -1
        self.leader_id = None
        
        # Lock manager untuk state machine (volatile)
        self.state_machine = LockManager()
        self.lock_manager = self.state_machine

        # State untuk leader
        self.next_index = {p: 0 for p in peers} 
        self.match_index = {p: -1 for p in peers}
        
        # State untuk election
        self.election_timer = None
        self.heartbeat_task = None
        self._next_election_timeout()

        # Muat state persisten dari Redis (sinkron di init)
        asyncio.run(self._load_persistent_state())


    def _next_election_timeout(self):
        """Menghitung election timeout acak antara MIN dan MAX (dalam detik)."""
        self.election_timeout = random.randrange(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX) / 1000.0
        self.last_contact_time = time.time()


    async def _load_persistent_state(self):
        """Memuat current_term, voted_for, dan log dari Redis."""
        try:
            # Muat Term
            term_str = await self.redis_client.get(KEY_TERM)
            if term_str:
                self.current_term = int(term_str)
            
            # Muat Voted For
            voted_for_bytes = await self.redis_client.get(KEY_VOTED_FOR)
            self.voted_for = voted_for_bytes.decode('utf-8') if voted_for_bytes else None

            print(f"[{self.node_id}] Persistent state loaded: Term={self.current_term}, VotedFor={self.voted_for}")
        except Exception as e:
            print(f"[{self.node_id}] ERROR loading persistent state: {e}")

    async def save_persistent_state(self):
        """Menyimpan current_term, voted_for, dan log ke Redis."""
        try:
            await self.redis_client.set(KEY_TERM, self.current_term)
            await self.redis_client.set(KEY_VOTED_FOR, self.voted_for if self.voted_for else "")
        except Exception as e:
            print(f"[{self.node_id}] ERROR saving persistent state: {e}")
            
    async def get_queue_status(self):
        """Mengembalikan statistik queue untuk monitoring cluster."""
        try:
            # Contoh simulasi queue internal (bisa disesuaikan dengan LockManager/StateMachine kamu)
            total_messages = len(self.state_machine.queue) if hasattr(self.state_machine, "queue") else 0
            processed = getattr(self.state_machine, "processed", 0)
            pending = max(total_messages - processed, 0)

            # Simulasi throughput (kalau belum ada metric real)
            throughput = f"{total_messages * 10} msg/sec" if total_messages > 0 else "unknown"

            # Kumpulkan status node lain
            nodes_status = {}
            for peer_id, (host, port) in self.peers.items():
                nodes_status[peer_id] = {
                    "messages": total_messages // len(self.peers) if self.peers else 0,
                    "status": "healthy"
                }

            # Termasuk node ini sendiri
            nodes_status[self.node_id] = {
                "messages": total_messages // (len(self.peers) + 1) if self.peers else total_messages,
                "status": self.role
            }

            return {
                "total_messages": total_messages,
                "processed": processed,
                "pending": pending,
                "throughput": throughput,
                "nodes": nodes_status
            }

        except Exception as e:
            print(f"[{self.node_id}] Error in get_queue_status: {e}")
            return {
                "total_messages": 0,
                "processed": 0,
                "pending": 0,
                "throughput": "unknown",
                "nodes": {}
            }



    # -------------------------
    # State Transition Helpers
    # -------------------------

    async def _become_follower(self, term, leader_id=None):
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
            self.heartbeat_task = None

        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            await self.save_persistent_state()

        self.state = "follower"
        self.leader_id = leader_id
        self._next_election_timeout()
        print(f"[{self.node_id}] Transitioned to FOLLOWER, Term {self.current_term}, Leader {self.leader_id}")

    async def _become_candidate(self):
        """Transisi ke state Candidate dan memulai election."""
        self.current_term += 1
        self.state = "candidate"
        self.voted_for = self.node_id
        self.leader_id = None
        self._next_election_timeout()
        await self.save_persistent_state()
        
        print(f"[{self.node_id}] State: CANDIDATE, Term: {self.current_term} - Starting election.")
        
        await self._send_request_vote()

    async def _become_leader(self):
        """Transisi ke state Leader."""
        self.state = "leader"
        self.leader_id = self.node_id
        
        self.next_index = {p: len(self.log) for p in self.peers}
        self.match_index = {p: -1 for p in self.peers}
        
        print(f"[{self.node_id}] State: LEADER, Term: {self.current_term}")
        
        await self._send_append_entries(is_heartbeat=True)
        
        self.heartbeat_task = asyncio.create_task(self._leader_heartbeat_loop())

    # -------------------------
    # Election Logic
    # -------------------------

    async def _leader_heartbeat_loop(self):
        """Loop Leader untuk mengirim AppendEntries (Heartbeat) secara berkala."""
        while self.state == "leader":
            await self._send_append_entries(is_heartbeat=True)
            await asyncio.sleep(HEARTBEAT_INTERVAL / 1000.0)
            
    async def _send_request_vote(self):
        """Mengirim RequestVote RPC ke semua peer."""
        if self.state != "candidate":
            return
            
        votes_received = 1
        majority = (len(self.peers) + 1) // 2 + 1
        
        last_log_index = len(self.log) - 1
        last_log_term = self.log[last_log_index]['term'] if self.log else 0

        rpc_data = {
            "rpc": "RequestVote",
            "term": self.current_term,
            "candidate_id": self.node_id,
            "last_log_index": last_log_index,
            "last_log_term": last_log_term,
        }
        
        responses = await self.broadcast_rpc("/message", rpc_data)
        
        for res in responses:
            if isinstance(res, dict) and res.get("vote_granted"):
                votes_received += 1
            
            if isinstance(res, dict) and res.get("term", 0) > self.current_term:
                await self._become_follower(res["term"])
                return
        
        if self.state == "candidate" and votes_received >= majority:
            await self._become_leader()
    
    # -------------------------
    # Log Replication (AppendEntries)
    # -------------------------
    
    async def _send_append_entries(self, is_heartbeat: bool):
        """Mengirim AppendEntries RPC ke semua peer."""
        if self.state != "leader":
            return

        for peer_id, _ in self.peers.items():
            entries_to_send = []
            prev_log_index = self.next_index[peer_id] - 1
            prev_log_term = self.log[prev_log_index]['term'] if prev_log_index >= 0 else 0
            
            if not is_heartbeat:
                entries_to_send = self.log[self.next_index[peer_id]:]

            rpc_data = {
                "rpc": "AppendEntries",
                "term": self.current_term,
                "leader_id": self.node_id,
                "prev_log_index": prev_log_index,
                "prev_log_term": prev_log_term,
                "entries": entries_to_send,
                "leader_commit": self.commit_index
            }

            asyncio.create_task(self._handle_append_entries_response(peer_id, rpc_data))
            
    async def _handle_append_entries_response(self, peer_id, rpc_data):
        """Memproses respons dari AppendEntries RPC."""
        response = await self.send_rpc(peer_id, "/message", rpc_data)
        
        if isinstance(response, dict):
            if response.get("term", 0) > self.current_term:
                await self._become_follower(response["term"])
                return
            
            if response.get("success"):
                if not rpc_data["entries"]:
                    return

                new_match_index = rpc_data["prev_log_index"] + len(rpc_data["entries"])
                self.match_index[peer_id] = new_match_index
                self.next_index[peer_id] = new_match_index + 1
                
                self._check_commit_index()
            else:
                self.next_index[peer_id] = max(0, self.next_index[peer_id] - 1)


    def _check_commit_index(self):
        """Leader: Cek apakah ada entry baru yang bisa di-commit."""
        if self.state != "leader":
            return
            
        majority = (len(self.peers) + 1) // 2 + 1
        
        for idx in range(len(self.log) - 1, self.commit_index, -1):
            if self.log[idx]['term'] != self.current_term:
                continue
                
            count = 1
            for peer_id in self.peers:
                if self.match_index[peer_id] >= idx:
                    count += 1
            
            if count >= majority:
                new_commit = idx
                
                if new_commit > self.commit_index:
                    self.commit_index = new_commit
                    print(f"[{self.node_id}] Committed up to index {self.commit_index}")
                    break
    async def apply_log_entry(self, entry):
        """Menerapkan satu log entry ke state machine (LockManager)"""
        try:
            data = json.loads(entry["command"])
            op = data.get("op")

            # --- Operasi LOCK ---
            if op == "lock":
                result = self.state_machine.acquire_lock(
                    resource=data["resource"],
                    client_id=data["client_id"],
                    lock_type=data.get("type", "exclusive")
                )
                print(f"[{self.node_id}] Applied LOCK → {result}")

            # --- Operasi RELEASE ---
            elif op == "release":
                result = self.state_machine.release_lock(
                    resource=data["resource"],
                    client_id=data["client_id"]
                )
                print(f"[{self.node_id}] Applied RELEASE → {result}")

            else:
                print(f"[{self.node_id}] Unknown op: {op}")
                result = "Unknown operation"

            return {"success": True, "result": result}

        except Exception as e:
            print(f"[{self.node_id}] ERROR applying log entry: {e}")
            return {"success": False, "error": str(e)}


    # -------------------------
    # Client Commands & Status
    # -------------------------
    
    def get_leader_address(self):
        """Return leader's host and port."""
        if self.leader_id and self.leader_id in self.peers:
            return self.peers[self.leader_id]
        return ("unknown", 0)
    
    async def handle_client_command(self, content):
        if self.state != "leader":
            host, port = self.get_leader_address()
            return {"is_redirect": True, "leader_id": self.leader_id, "leader_address": f"{host}:{port}"}

        entry = {"term": self.current_term, "command": content}
        self.log.append(entry)
        await self.save_persistent_state()
        idx = len(self.log) - 1
        
        await self._send_append_entries(is_heartbeat=False)
        
        return {"success": True, "log_index": idx, "message": "Appended, replication triggered."}

    async def get_node_status(self):
        """Mengembalikan status node saat ini."""
        status = {
            "node_id": self.node_id,
            "role": self.state,
            "term": self.current_term,
            "voted_for": self.voted_for,
            "leader_id": self.leader_id,
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "log_length": len(self.log),
            "next_election_timeout": self.election_timeout,
            "time_since_last_contact": time.time() - self.last_contact_time,
            "peers": list(self.peers.keys())
        }
        return status

    # -------------------------
    # RPC Handlers
    # -------------------------

    async def handle_message_rpc(self, data: dict) -> dict:
        """Handler RPC utama yang memproses RequestVote dan AppendEntries."""
        rpc_type = data.get("rpc")
        
        if data.get("term", 0) > self.current_term:
            await self._become_follower(data["term"], leader_id=data.get("leader_id"))
        
        if data.get("term", 0) < self.current_term:
            return {"term": self.current_term, "success": False}

        if rpc_type == "RequestVote":
            return await self._handle_request_vote(data)
        elif rpc_type == "AppendEntries":
            if self.state == "follower":
                 self.leader_id = data.get("leader_id")
            return await self._handle_append_entries(data)
        
        return {"term": self.current_term, "success": False, "message": "Unknown RPC type"}

    async def _handle_request_vote(self, data: dict) -> dict:
        """Memproses RequestVote RPC dari Candidate."""
        candidate_id = data.get("candidate_id")
        candidate_term = data.get("term")
        
        vote_granted = False
        
        term_ok = (candidate_term == self.current_term and (self.voted_for is None or self.voted_for == candidate_id))
        
        last_log_index = len(self.log) - 1
        last_log_term = self.log[last_log_index]['term'] if last_log_index >= 0 else 0
        
        candidate_log_ok = (data["last_log_term"] > last_log_term) or \
                           (data["last_log_term"] == last_log_term and data["last_log_index"] >= last_log_index)

        if term_ok and candidate_log_ok:
            self.voted_for = candidate_id
            await self.save_persistent_state()
            self.last_contact_time = time.time()
            vote_granted = True
            print(f"[{self.node_id}] Voted for {candidate_id} in term {self.current_term}")

        return {"term": self.current_term, "vote_granted": vote_granted}

    async def _handle_append_entries(self, data: dict) -> dict:
        """Memproses AppendEntries RPC (termasuk Heartbeat)."""
        leader_id = data.get("leader_id")
        leader_term = data.get("term")
        
        self.last_contact_time = time.time()
        self.leader_id = leader_id

        if self.state != "follower" and leader_term >= self.current_term:
            await self._become_follower(leader_term, leader_id)
        
        return {"term": self.current_term, "success": True}

    # -------------------------
    # Core Loop
    # -------------------------

    async def _raft_loop(self):
        """Logika inti Raft State Machine."""
        while True:
            await asyncio.sleep(0.01)
            
            time_since_last_contact = time.time() - self.last_contact_time
            
            if self.state != "leader" and time_since_last_contact >= self.election_timeout:
                self._next_election_timeout()
                print(f"[{self.node_id}] Election timeout ({self.election_timeout:.3f}s) expired. Initiating election.")
                await self._become_candidate()


    async def run(self):
        """Memulai AIOHTTP server dan Raft core loop."""
        await self.start_server()
        await self._raft_loop()