import asyncio
import random
import time
import json
from aiohttp import web

from src.nodes.base_node import BaseNode
from src.nodes.lock_manager import LockManager

# Konstanta waktu (ms)
ELECTION_TIMEOUT_MIN = 300
ELECTION_TIMEOUT_MAX = 600
HEARTBEAT_INTERVAL = 150
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

        # State untuk leader
        self.next_index = {p: 0 for p in peers}
        self.match_index = {p: -1 for p in peers}

        # Komponen tambahan
        self.lock_manager = LockManager()
        self.pending_futures = {}

        # Timer election
        self.election_task = None
        self.heartbeat_task = None
        self.reset_election_timer()

        # Daftarkan endpoint RPC
        try:
            self.app.router.add_post("/message", self.handle_rpc_request)
        except Exception:
            pass

        print(f"[{self.node_id}] RaftNode initialized as {self.state}")

    # -------------------------
    # Utilities
    # -------------------------
    def reset_election_timer(self):
        self.election_timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX) / 1000.0
        self.last_heartbeat = time.time()

    def get_leader_address(self):
        if not self.leader_id:
            return (None, None)
        return self.peers.get(self.leader_id, (None, None))

    async def load_persistent_state(self):
        try:
            if hasattr(self, "redis_client") and self.redis_client:
                term = await self.redis_client.get(KEY_TERM)
                if term:
                    self.current_term = int(term)
                voted = await self.redis_client.get(KEY_VOTED_FOR)
                if voted and voted.decode():
                    self.voted_for = voted.decode()
                log_data = await self.redis_client.get(KEY_LOG)
                if log_data:
                    self.log = json.loads(log_data)
            print(f"[{self.node_id}] Loaded persistent state: term={self.current_term}, log_len={len(self.log)}")
        except Exception as e:
            print(f"[{self.node_id}] Failed loading persistent state: {e}")

    async def save_persistent_state(self):
        try:
            if hasattr(self, "redis_client") and self.redis_client:
                await self.redis_client.set(KEY_TERM, str(self.current_term))
                await self.redis_client.set(KEY_VOTED_FOR, self.voted_for or "")
                await self.redis_client.set(KEY_LOG, json.dumps(self.log))
        except Exception as e:
            print(f"[{self.node_id}] Failed saving persistent state: {e}")

    # -------------------------
    # RPC entry points
    # -------------------------
    async def handle_message_rpc(self, message):
        try:
            data = await message.json() if not isinstance(message, dict) else message
        except Exception:
            data = message
        return await self.handle_message(None, data)

    async def handle_rpc_request(self, request):
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"error": "invalid json"}, status=400)

        resp = await self.handle_message(request, data)
        if isinstance(resp, dict):
            resp["term"] = self.current_term
        return web.json_response(resp)

    # -------------------------
    # Main loop
    # -------------------------
    async def run(self):
        await self.load_persistent_state()
        await self.start_server()
        self.reset_election_timer()
        self._start_election_timer()
        print(f"[{self.node_id}] Raft core started as {self.state} (term {self.current_term})")

        try:
            while True:
                await self.check_and_apply_logs()
                await asyncio.sleep(0.05)
        finally:
            await self._cancel_tasks()

    async def _cancel_tasks(self):
        for t in [self.election_task, self.heartbeat_task]:
            if t and not t.done():
                t.cancel()

    # -------------------------
    # Election timers
    # -------------------------
    def _start_election_timer(self):
        if self.election_task and not self.election_task.done():
            self.election_task.cancel()
        self.election_task = asyncio.create_task(self._election_timeout_loop())

    async def _election_timeout_loop(self):
        while True:
            await asyncio.sleep(self.election_timeout)
            if self.state != "leader" and (time.time() - self.last_heartbeat) >= self.election_timeout:
                print(f"[{self.node_id}] Election timeout -> start election (term {self.current_term + 1})")
                await self._start_election()
            self.election_timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX) / 1000.0

    def _start_heartbeat_loop(self):
        if self.heartbeat_task and not self.heartbeat_task.done():
            self.heartbeat_task.cancel()
        self.heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def _heartbeat_loop(self):
        while self.state == "leader":
            await self._send_append_entries(is_heartbeat=True)
            await asyncio.sleep(HEARTBEAT_INTERVAL / 1000.0)

    # -------------------------
    # Election logic
    # -------------------------
    async def _start_election(self):
        self.state = "candidate"
        self.current_term += 1
        self.voted_for = self.node_id
        await self.save_persistent_state()
        self.vote_count = 1
        self.leader_id = None
        self.reset_election_timer()
        print(f"[{self.node_id}] Became CANDIDATE for term {self.current_term}")

        last_idx = len(self.log) - 1
        last_term = self.log[last_idx]["term"] if last_idx >= 0 else 0
        payload = {
            "candidate_id": self.node_id,
            "last_log_index": last_idx,
            "last_log_term": last_term
        }

        responses = await self._broadcast_rpc("RequestVote", payload)
        for r in responses:
            if isinstance(r, dict) and r.get("term", 0) > self.current_term:
                await self._step_down(r["term"])
                return

        for r in responses:
            if isinstance(r, dict) and (r.get("vote_granted") or r.get("voteGranted")):
                self.vote_count += 1

        total = len(self.peers) + 1
        majority = total // 2 + 1
        if self.vote_count >= majority:
            print(f"[{self.node_id}] Won election (votes={self.vote_count}/{total}), becoming leader")
            await self._become_leader()
        else:
            print(f"[{self.node_id}] Lost election ({self.vote_count}/{total}). Back to follower.")
            await self._step_down(self.current_term)

    async def _become_leader(self):
        self.state = "leader"
        self.leader_id = self.node_id
        last_index = len(self.log)
        for p in self.peers:
            self.next_index[p] = last_index
            self.match_index[p] = -1
        self._start_heartbeat_loop()
        await self._send_append_entries(is_heartbeat=True)
        print(f"[{self.node_id}] Became LEADER (term {self.current_term})")

    async def _step_down(self, new_term, leader_id=None):
        if new_term > self.current_term:
            self.current_term = new_term
        self.state = "follower"
        self.voted_for = None
        self.leader_id = leader_id
        await self.save_persistent_state()
        self.reset_election_timer()
        print(f"[{self.node_id}] Stepped down to follower. term={self.current_term}, leader={self.leader_id}")

    # -------------------------
    # RPC handlers
    # -------------------------
    async def handle_message(self, request, data):
        msg_type = data.get("type")
        incoming_term = data.get("term", 0)
        if incoming_term > self.current_term:
            await self._step_down(incoming_term)

        if msg_type in ("RequestVote", "request_vote"):
            return await self.handle_request_vote(data)
        if msg_type in ("AppendEntries", "append_entries"):
            return await self.handle_append_entries(data)
        if msg_type in ("ClientCommand", "client_command"):
            return await self.handle_client_command(data)

        return {"term": self.current_term, "success": False, "message": "unknown rpc type"}

    async def handle_request_vote(self, content):
        term = content.get("term", self.current_term)
        candidate = content.get("candidate_id")
        last_log_index = content.get("last_log_index", -1)
        last_log_term = content.get("last_log_term", 0)
        resp = {"term": self.current_term, "vote_granted": False}

        if term < self.current_term:
            return resp

        if term > self.current_term:
            await self._step_down(term)

        if self.voted_for is None or self.voted_for == candidate:
            my_last_term = self.log[-1]["term"] if self.log else 0
            my_last_index = len(self.log) - 1
            up_to_date = (last_log_term > my_last_term) or \
                         (last_log_term == my_last_term and last_log_index >= my_last_index)
            if up_to_date:
                self.voted_for = candidate
                await self.save_persistent_state()
                self.reset_election_timer()
                resp["vote_granted"] = True
                print(f"[{self.node_id}] Voted for {candidate} in term {self.current_term}")
        return resp

    async def handle_append_entries(self, content):
        term = content.get("term", self.current_term)
        leader_id = content.get("leader_id")
        prev_index = content.get("prev_log_index", -1)
        prev_term = content.get("prev_log_term", 0)
        entries = content.get("entries", [])
        leader_commit = content.get("leader_commit", -1)
        resp = {"term": self.current_term, "success": False}

        if term < self.current_term:
            return resp

        if term > self.current_term:
            await self._step_down(term, leader_id=leader_id)
        else:
            self.reset_election_timer()
            self.leader_id = leader_id

        match = (prev_index == -1) or (0 <= prev_index < len(self.log) and self.log[prev_index]["term"] == prev_term)
        if not match:
            return resp

        if entries:
            next_idx = prev_index + 1
            for i, ent in enumerate(entries):
                if next_idx + i < len(self.log):
                    self.log[next_idx + i] = ent
                else:
                    self.log.append(ent)
            await self.save_persistent_state()

        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log) - 1)

        resp["success"] = True
        resp["match_index"] = len(self.log) - 1
        return resp

    # -------------------------
    # Replication helpers
    # -------------------------
    async def _send_append_entries(self, is_heartbeat=False):
        tasks = []
        for peer in self.peers:
            next_idx = self.next_index.get(peer, len(self.log))
            prev_idx = next_idx - 1
            prev_term = self.log[prev_idx]["term"] if prev_idx >= 0 and prev_idx < len(self.log) else 0
            entries = self.log[next_idx:]

            content = {
                "leader_id": self.node_id,
                "prev_log_index": prev_idx,
                "prev_log_term": prev_term,
                "entries": entries,
                "leader_commit": self.commit_index
            }
            tasks.append(self._send_rpc(peer, "AppendEntries", content))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for peer, r in zip(self.peers.keys(), results):
            if isinstance(r, dict):
                if r.get("term", 0) > self.current_term:
                    await self._step_down(r["term"])
                    return
                if r.get("success"):
                    match_idx = r.get("match_index", -1)
                    self.match_index[peer] = max(self.match_index.get(peer, -1), match_idx)
                    self.next_index[peer] = self.match_index[peer] + 1
        await self.commit_logs_on_majority()

    async def _send_rpc(self, peer_id, msg_type, content):
        payload = {**content, "type": msg_type, "term": self.current_term, "sender": self.node_id}
        try:
            return await self.send_message(peer_id, payload)
        except Exception as e:
            return {"error": str(e)}

    async def _broadcast_rpc(self, msg_type, content):
        tasks = [self._send_rpc(peer, msg_type, content) for peer in self.peers]
        return await asyncio.gather(*tasks, return_exceptions=True)

    async def commit_logs_on_majority(self):
        N = len(self.log)
        new_commit = self.commit_index
        majority = (len(self.peers) + 1) // 2 + 1
        for idx in range(self.commit_index + 1, N):
            if self.log[idx]["term"] != self.current_term:
                continue
            count = 1
            for p in self.peers:
                if self.match_index.get(p, -1) >= idx:
                    count += 1
            if count >= majority:
                new_commit = idx
        if new_commit > self.commit_index:
            self.commit_index = new_commit
            print(f"[{self.node_id}] Committed up to index {self.commit_index}")

    # -------------------------
    # Client Commands & Status
    # -------------------------
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
        try:
            return {
                "node_id": self.node_id,
                "role": self.state,  # ✅ fixed
                "term": self.current_term,
                "voted_for": self.voted_for,
                "leader_id": self.leader_id,
                "commit_index": self.commit_index,
                "last_applied": self.last_applied,
                "log_length": len(self.log),
                "peers": list(self.peers.keys()),
            }
        except Exception as e:
            print(f"[{self.node_id}] ERROR get_node_status: {e}")
            return {"error": str(e)}
