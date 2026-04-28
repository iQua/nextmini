import pickle
import time
import io
import os
import json
import tempfile
from pathlib import Path
import torch
import torch.nn.functional as F
from torch.optim import AdamW
from . import config
from .dataset import GSM8KLoader, is_correct
from .model_loader import load_policy_model, load_tokenizer
import threading
from tqdm import tqdm

try:
    import nextmini_py as nm
except ImportError as exc:
    raise SystemExit(
        "nextmini_py is not installed. Build the wheel with:\n"
        "  maturin build --release -m python-api/Cargo.toml\n"
        "  pip install target/wheels/nextmini_py-*.whl"
    ) from exc


def configured_model_dtype():
    raw = config.MODEL_DTYPE.strip().lower()
    if raw in {"", "auto"}:
        return "auto"
    aliases = {
        "fp16": torch.float16,
        "float16": torch.float16,
        "half": torch.float16,
        "bf16": torch.bfloat16,
        "bfloat16": torch.bfloat16,
        "fp32": torch.float32,
        "float32": torch.float32,
    }
    if raw not in aliases:
        raise ValueError(f"unsupported MODEL_DTYPE={config.MODEL_DTYPE!r}")
    return aliases[raw]


class Trainer:
    def __init__(self, config_path=None):
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        print(f"Trainer initializing on {self.device} with model {config.MODEL_NAME}...")
        
        # Load Models
        model_dtype = configured_model_dtype()
        self.policy_model = load_policy_model(config.MODEL_NAME, dtype=model_dtype).to(self.device)
        
        # Reference model (frozen)
        self.ref_model = load_policy_model(config.MODEL_NAME, dtype=model_dtype).to(self.device)
        self.ref_model.eval()
        
        self.tokenizer = load_tokenizer(config.MODEL_NAME)
        
        self.optimizer = AdamW(self.policy_model.parameters(), lr=config.LEARNING_RATE)
        
        self.dataset = GSM8KLoader("train")
        
        # Initialize nextmini dataplane
        self.config_path = config_path if config_path else config.TRAINER_CONFIG
        print(f"Initializing nextmini dataplane with config: {self.config_path}")
        self.dataplane = nm.Dataplane(self.config_path)
        info = self.dataplane.get_network_info()
        self.user_space_address = info["user_space_address"]
        self.node_id = int(info["node_id"])
        
        # Setup connections to workers
        self.worker_connections = []
        self.worker_locks = []
        
        for i, worker_node_id in enumerate(config.WORKER_NODE_IDS):
            worker_port = config.WORKER_BASE_PORT + i
            print(f"Setting up connection to Worker {i} (node {worker_node_id}, port {worker_port})")
            
            # Register receiver for this worker
            receiver = self.dataplane.register_receiver_from_node(
                src_node_id=worker_node_id,
                src_port=worker_port,
                dst_port=config.TRAINER_PORT,
            )
            
            self.worker_connections.append({
                'node_id': worker_node_id,
                'port': worker_port,
                'receiver': receiver
            })
            self.worker_locks.append(threading.Lock())

        # Wait for topology to be ready (all nodes connected and routes installed)
        print("Waiting for topology to be ready...")
        self.dataplane.wait_for_topology_ready()
        print("Topology is ready!")
        
        # Create Multicast Group
        print(f"Creating multicast group '{config.MULTICAST_GROUP_NAME}'...")
        self.dataplane.create_group(config.MULTICAST_GROUP_NAME)
        self.group_id, self.group_ip, _ = self.dataplane.group_is_ready(timeout_ms=30000)
        print(f"Multicast group ready: ID={self.group_id}, IP={self.group_ip}")

        receiver_ids = [conn["node_id"] for conn in self.worker_connections]
        edges, throughput = self._compute_multicast_routes(receiver_ids)
        self.dataplane.set_group_routes(self.group_id, edges)
        if throughput is not None:
            print(f"Applied LP multicast routes (throughput={throughput:.3f})")
        print(f"Installed multicast routes for group {self.group_id} ({len(edges)} edges)")
        
        print(f"Trainer ready with {len(self.worker_connections)} workers")

    def _compute_multicast_routes(self, receiver_ids):
        controller_path = Path(config.CONTROLLER_CONFIG)
        if not controller_path.is_absolute():
            controller_path = (Path(__file__).resolve().parents[3] / controller_path).resolve()
        if not controller_path.is_file():
            raise RuntimeError(f"Controller config not found: {controller_path}")

        try:
            from examples.lp.solver import (
                build_graph_from_controller_config,
                compute_mflow_tree_edges,
                load_toml,
            )
        except ImportError as exc:
            raise RuntimeError(
                "LP solver unavailable. Ensure examples/lp dependencies are installed."
            ) from exc

        graph = build_graph_from_controller_config(str(controller_path))

        if getattr(config, "MULTICAST_PROBE_LINKS", False):
            try:
                from examples.lp.main import (
                    _apply_link_rates,
                    _connect_db,
                    _db_settings,
                    _fetch_link_rates_from_probes,
                    _request_link_probes,
                    _wait_for_probe_finish,
                )
            except ImportError as exc:
                raise RuntimeError(
                    "MULTICAST_PROBE_LINKS=true requires examples/lp/main.py DB helpers."
                ) from exc

            controller_cfg = load_toml(str(controller_path))
            settings = _db_settings(controller_cfg)
            conn = _connect_db(settings)
            try:
                print(
                    f"Probing {len(graph.edges)} links ({config.MULTICAST_PROBE_BYTES} bytes each)...",
                    flush=True,
                )
                probe_ids = _request_link_probes(conn, graph.edges, bytes_per_flow=config.MULTICAST_PROBE_BYTES)
                ok = _wait_for_probe_finish(conn, probe_ids, timeout_secs=config.MULTICAST_PROBE_TIMEOUT_SECS)
                if not ok:
                    print(
                        f"warning: probe timed out after {config.MULTICAST_PROBE_TIMEOUT_SECS}s; using completed probes only",
                        flush=True,
                    )
                rates = _fetch_link_rates_from_probes(conn, probe_ids)
                _apply_link_rates(graph, rates)
                graph.adj = graph._build_adjacency()
                print(f"Applied {len(rates)} probed link capacities.", flush=True)
            finally:
                conn.close()

        edges, throughput = compute_mflow_tree_edges(
            graph,
            src=self.node_id,
            destinations=receiver_ids,
        )
        if not edges:
            raise RuntimeError(
                f"LP solver returned no edges for src={self.node_id} dests={receiver_ids}"
            )

        return edges, throughput

    def accept_workers(self, num_workers=2):
        """Wait for handshake from all workers"""
        print(f"Waiting for {num_workers} workers to send handshake...", flush=True)
        
        # We need to accept handshakes from ANY worker, not just in order 0, 1, 2...
        # Because network arrival time is non-deterministic.
        
        connected_workers = set()
        
        while len(connected_workers) < num_workers:
            # Poll all workers
            found_new = False
            for i in range(num_workers):
                if i in connected_workers:
                    continue
                    
                # Try to receive with a short timeout to poll
                try:
                    # We use a short timeout to cycle through workers
                    msg = self.recv_from_worker(i, timeout_ms=100) 
                    if msg:
                        if msg.get("type") == "HANDSHAKE":
                            rank = msg['rank']
                            print(f"Worker rank {rank} (node {config.WORKER_NODE_IDS[rank]}) identified", flush=True)
                            if rank != i:
                                print(f"Warning: Worker {i} connection received handshake claiming rank {rank}", flush=True)
                            self.send_to_worker(i, {
                                "type": "HANDSHAKE_ACK",
                                "rank": rank,
                                "trainer_user_ip": self.user_space_address,
                            })
                            connected_workers.add(rank)
                            found_new = True
                        else:
                            print(f"Received unexpected message from worker {i}: {msg}", flush=True)
                except Exception as e:
                    # Ignore timeout errors during polling or if recv returns None (disconnect)
                    pass
            
            if not found_new:
                time.sleep(0.1)
                
        print("All workers connected.", flush=True)

    def send_to_worker(self, worker_idx: int, data: dict):
        """Send message to specific worker"""
        conn = self.worker_connections[worker_idx]
        serialized = pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL)
        view = nm.PacketView(serialized)
        
        self.dataplane.send_to_node(
            dst_node_id=conn['node_id'],
            frozen=view,
            src_port=config.TRAINER_PORT,
            dst_port=conn['port'],
        )
    
    def recv_from_worker(self, worker_idx: int, timeout_ms: int = 30000, expected_type: str = None):
        """Receive message from specific worker, optionally filtering by type"""
        conn = self.worker_connections[worker_idx]
        
        start_time = time.time()
        
        while True:
            # Calculate remaining timeout
            elapsed = (time.time() - start_time) * 1000
            remaining = max(1, int(timeout_ms - elapsed))
            
            delivery = conn['receiver'].recv(timeout_ms=remaining)
            
            if delivery is None:
                return None
            
            msg = pickle.loads(delivery.payload)
            
            # If no type filtering is requested, return immediately
            if expected_type is None:
                return msg
                
            # Check if this is the message we are waiting for
            if msg.get("type") == expected_type:
                return msg
            
            # Otherwise, it's a stale/unexpected message. Log and continue.
            print(f"Worker {worker_idx}: Ignoring unexpected message type '{msg.get('type')}' (expected '{expected_type}'). Content: {msg}", flush=True)
            
            if remaining <= 1:
                return None

    def broadcast_weights(self):
        """Broadcast model weights to all workers via Multicast"""
        print("Broadcasting weights to workers via Multicast...")
        
        # ============ TIMING: Weight Broadcast Start ============
        weight_broadcast_start = time.time()
        
        state_dict = self.policy_model.state_dict()
        # Move to CPU for serialization
        state_dict_cpu = {k: v.cpu() for k, v in state_dict.items()}
        
        buffer = io.BytesIO()
        torch.save(state_dict_cpu, buffer)
        data_bytes = buffer.getvalue()
        size = len(data_bytes)
        
        print(f"Serialized weights: {size} bytes ({size/1024/1024:.2f} MB)")
        
        # 1. Send Metadata and Wait for Ready
        receiver_ids = []
        for i in range(len(self.worker_connections)):
            receiver_ids.append(self.worker_connections[i]['node_id'])
        
        errors = []
        def handshake_worker(i):
            with self.worker_locks[i]:
                # Send Metadata
                print(f"Trainer sending WEIGHT_METADATA to Worker {i} (node {self.worker_connections[i]['node_id']}, port {self.worker_connections[i]['port']})...", flush=True)
                self.send_to_worker(i, {
                    "type": "WEIGHT_METADATA",
                    "group_id": self.group_id,
                    "group_ip": self.group_ip,
                    "size": size,
                    "src_node_id": config.TRAINER_NODE_ID
                })
                print(f"Trainer sent WEIGHT_METADATA to Worker {i}, waiting for READY...", flush=True)
                
                # Wait for READY
                try:
                    msg = self.recv_from_worker(i, timeout_ms=120000, expected_type="READY_FOR_MULTICAST")
                    
                    if not msg:
                        raise RuntimeError(f"Worker {i} failed to reply READY_FOR_MULTICAST within timeout")
                    else:
                        print(f"Worker {i} replied READY_FOR_MULTICAST", flush=True)
                except Exception as e:
                    errors.append(e)
                    raise e
        
        print(f"Starting {len(self.worker_connections)} handshake threads...", flush=True)
        threads = []
        for i in range(len(self.worker_connections)):
            t = threading.Thread(target=handshake_worker, args=(i,), name=f"HandshakeWorker-{i}")
            t.start()
            threads.append(t)
            
        for t in threads:
            t.join()
            
        if errors:
            raise RuntimeError(f"Handshake failed: {errors}")
            
        # 2. Send Data Lossless
        print(f"Starting lossless multicast of {size} bytes to {receiver_ids}...")
        builder = nm.PacketBuilder(size=size)
        builder.write(data_bytes)
        view = builder.freeze()
        sid = self.dataplane.send_data(
            self.group_id,
            self.group_ip,
            receiver_ids,
            view,
            block_size=config.CHUNK_SIZE,
            src_port=config.TRAINER_PORT,
            dst_port=config.WORKER_BASE_PORT # All workers listen on BASE_PORT for multicast
        )
        
        print(f"Waiting for multicast transfer (SID={sid})...")
        multicast_transfer_start = time.time()
        ok = self.dataplane.lossless_wait(sid, timeout_ms=config.MULTICAST_TIMEOUT_MS)
        multicast_transfer_end = time.time()
        print(f"Multicast completion: {ok}")
        
        # ============ TIMING: Weight Broadcast End ============
        weight_broadcast_end = time.time()
        total_broadcast_time = weight_broadcast_end - weight_broadcast_start
        actual_transfer_time = multicast_transfer_end - multicast_transfer_start
        throughput_gbps = (size * 8 / 1e9) / actual_transfer_time if actual_transfer_time > 0 else 0
        
        print(f"\n{'='*60}")
        print(f"WEIGHT BROADCAST TIMING (Trainer → Workers):")
        print(f"  Total time (incl. handshake): {total_broadcast_time:.3f}s")
        print(f"  Actual multicast transfer:     {actual_transfer_time:.3f}s")
        print(f"  Data size:                     {size/1024/1024:.2f} MB")
        print(f"  Throughput:                    {throughput_gbps:.3f} Gbps")
        print(f"{'='*60}\n")
        
        return {
            'total_time': total_broadcast_time,
            'transfer_time': actual_transfer_time,
            'size_bytes': size,
            'throughput_gbps': throughput_gbps
        }

    def broadcast_weights_sharded(self):
        """Broadcast model weights using sharded checkpoints for large models.
        
        This method saves the model as multiple shard files and sends them
        one by one, avoiding OOM for 32GB+ models.
        """
        print("Broadcasting weights using SHARDED CHECKPOINT mode...")
        
        weight_broadcast_start = time.time()
        
        receiver_ids = [conn['node_id'] for conn in self.worker_connections]
        
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            
            # 1. Save model as sharded checkpoint
            print(f"Saving model as sharded checkpoint (max_shard_size={config.SHARD_SIZE})...")
            save_start = time.time()
            self.policy_model.save_pretrained(
                str(tmpdir),
                max_shard_size=config.SHARD_SIZE,
                safe_serialization=True
            )
            save_time = time.time() - save_start
            print(f"Model saved in {save_time:.2f}s")
            
            # 2. Find shard files and index
            shard_files = sorted(tmpdir.glob("*.safetensors"))
            index_file = tmpdir / "model.safetensors.index.json"
            
            # Check if model is actually sharded (small models won't be)
            if not index_file.exists():
                # Single file mode - model is too small for sharding
                print("Model is not sharded (too small). Using single file mode.")
                shard_files = sorted(tmpdir.glob("*.safetensors"))
                num_shards = len(shard_files)
                index_data = None
            else:
                with open(index_file, 'r') as f:
                    index_data = f.read()
                num_shards = len(shard_files)
                print(f"Model sharded into {num_shards} files")
            
            # Read config.json (required for from_pretrained)
            config_file = tmpdir / "config.json"
            if config_file.exists():
                with open(config_file, 'r') as f:
                    config_json_data = f.read()
            else:
                raise RuntimeError("config.json not found in saved checkpoint")
            
            # 3. Notify workers about sharded transfer
            total_size = sum(f.stat().st_size for f in shard_files)
            shard_names = [f.name for f in shard_files]
            shard_sizes = {f.name: f.stat().st_size for f in shard_files}
            shard_sizes["config.json"] = len(config_json_data.encode('utf-8'))
            if index_data is not None:
                shard_sizes["index"] = len(index_data.encode('utf-8'))
            
            errors = []
            def handshake_worker(i):
                with self.worker_locks[i]:
                    self.send_to_worker(i, {
                        "type": "WEIGHT_METADATA_SHARDED",
                        "group_id": self.group_id,
                        "group_ip": self.group_ip,
                        "num_shards": num_shards,
                        "shard_names": shard_names,
                        "shard_sizes": shard_sizes,
                        "has_index": index_data is not None,
                        "total_size": total_size,
                        "src_node_id": config.TRAINER_NODE_ID
                    })
                    try:
                        msg = self.recv_from_worker(i, timeout_ms=120000, 
                                                    expected_type="READY_FOR_SHARDED_MULTICAST")
                        if not msg:
                            raise RuntimeError(f"Worker {i} failed to reply READY")
                        print(f"Worker {i} ready for sharded transfer", flush=True)
                    except Exception as e:
                        errors.append(e)
                        raise e
            
            # Parallel handshake
            threads = [threading.Thread(target=handshake_worker, args=(i,)) 
                       for i in range(len(self.worker_connections))]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            if errors:
                raise RuntimeError(f"Handshake failed: {errors}")
            
            # 4. Send config.json first (required for from_pretrained)
            transfer_start = time.time()
            print("Sending config.json...")
            config_bytes = config_json_data.encode('utf-8')
            self._send_shard_data("config.json", config_bytes, receiver_ids)
            
            # 5. Send index (if exists)
            if index_data is not None:
                print("Sending index file...")
                index_bytes = index_data.encode('utf-8')
                self._send_shard_data("index", index_bytes, receiver_ids)
            
            # 6. Send each shard
            for shard_path in shard_files:
                shard_data = shard_path.read_bytes()
                print(f"Sending shard {shard_path.name} ({len(shard_data)/1024/1024:.1f} MB)...")
                self._send_shard_data(shard_path.name, shard_data, receiver_ids)
            
            transfer_end = time.time()
        
        weight_broadcast_end = time.time()
        total_time = weight_broadcast_end - weight_broadcast_start
        transfer_time = transfer_end - transfer_start
        throughput_gbps = (total_size * 8 / 1e9) / transfer_time if transfer_time > 0 else 0
        
        print(f"\n{'='*60}")
        print(f"SHARDED WEIGHT BROADCAST TIMING:")
        print(f"  Total time (incl. save):       {total_time:.3f}s")
        print(f"  Save checkpoint time:          {save_time:.3f}s")
        print(f"  Transfer time:                 {transfer_time:.3f}s")
        print(f"  Total data size:               {total_size/1024/1024:.2f} MB")
        print(f"  Number of shards:              {num_shards}")
        print(f"  Throughput:                    {throughput_gbps:.3f} Gbps")
        print(f"{'='*60}\n")
        
        return {
            'total_time': total_time,
            'transfer_time': transfer_time,
            'size_bytes': total_size,
            'throughput_gbps': throughput_gbps,
            'num_shards': num_shards
        }

    def _send_shard_data(self, name: str, data: bytes, receiver_ids: list):
        """Send a single shard via multicast."""
        builder = nm.PacketBuilder(size=len(data))
        builder.write(data)
        view = builder.freeze()
        
        sid = self.dataplane.send_data(
            self.group_id,
            self.group_ip,
            receiver_ids,
            view,
            block_size=config.CHUNK_SIZE,
            src_port=config.TRAINER_PORT,
            dst_port=config.WORKER_BASE_PORT
        )
        
        ok = self.dataplane.lossless_wait(sid, timeout_ms=config.MULTICAST_TIMEOUT_MS)
        if not ok:
            raise RuntimeError(f"Failed to send shard {name}")

    def train_step(self, batch):
        """Execute one training step"""
        # 1. Broadcast current weights (choose mode based on config)
        if config.USE_SHARDED_WEIGHTS:
            weight_metrics = self.broadcast_weights_sharded()
        else:
            weight_metrics = self.broadcast_weights()

        if config.BROADCAST_ONLY:
            print("BROADCAST_ONLY=true; skipping rollouts and optimizer update.")
            return {"weight_metrics": weight_metrics, "broadcast_only": True}

        # 2. Send prompts to workers
        prompts = [item["question"] for item in batch]
        ground_truths = [item["answer"] for item in batch]
        
        # Split prompts among workers
        chunk_size = len(prompts) // len(self.worker_connections)
        worker_batches = []
        for i in range(len(self.worker_connections)):
            start = i * chunk_size
            end = (i + 1) * chunk_size if i < len(self.worker_connections) - 1 else len(prompts)
            worker_batches.append(prompts[start:end])
        
        # Request rollouts
        worker_results = [None] * len(self.worker_connections)
        rollout_times = [None] * len(self.worker_connections)
        network_times = [None] * len(self.worker_connections)
        rollout_sizes = [0] * len(self.worker_connections)
        
        # ============ TIMING: Rollout Request Start ============
        rollout_start_time = time.time()
        
        def query_worker(i, p_batch):
            with self.worker_locks[i]:
                worker_start = time.time()
                self.send_to_worker(i, {"type": "ROLLOUT", "prompts": p_batch})
                # Use a long timeout for rollouts as generation can be slow
                meta = self.recv_from_worker(i, timeout_ms=60000, expected_type="ROLLOUT_METADATA")
                if not meta:
                    print(f"Trainer: did not receive ROLLOUT_METADATA from worker {i}", flush=True)
                    return
                size = meta.get("size")
                if not isinstance(size, int) or size <= 0:
                    print(f"Trainer: invalid ROLLOUT_METADATA from worker {i}: {meta}", flush=True)
                    return

                rollout_sizes[i] = size

                worker_node_id = self.worker_connections[i]['node_id']
                worker_port = self.worker_connections[i]['port']
                rollout_group_id = config.rollout_group_id(worker_node_id)

                try:
                    # IMPORTANT: Register receiver FIRST, before signaling worker
                    sid = self.dataplane.receive_data(
                        rollout_group_id,
                        worker_node_id,
                        src_port=worker_port,
                        dst_port=config.TRAINER_PORT,
                    )

                    # Now signal worker that we're ready to receive
                    self.send_to_worker(i, {"type": "READY_FOR_ROLLOUT_DATA"})

                    transfer_start = time.time()
                    ok = self.dataplane.lossless_wait(sid, timeout_ms=config.MULTICAST_TIMEOUT_MS)
                    transfer_end = time.time()
                except Exception as e:
                    print(f"Trainer: error receiving lossless rollout data from worker {i}: {e}", flush=True)
                    return

                if not ok:
                    print(f"Trainer: lossless rollout transfer from worker {i} did not complete successfully", flush=True)
                    return

                frozen = self.dataplane.get_data_buffer(sid)
                payload_bytes = bytes(frozen.read())
                try:
                    result = pickle.loads(payload_bytes)
                except Exception as e:
                    print(f"Trainer: failed to decode rollout payload from worker {i}: {e}", flush=True)
                    return

                worker_results[i] = result

                # End-to-end time for this worker (send ROLLOUT → payload received and decoded)
                recv_time = transfer_end
                rollout_times[i] = recv_time - worker_start

                # Pure network transfer time on the trainer side: duration of the
                # lossless session after the first frame arrived.
                network_times[i] = max(transfer_end - transfer_start, 0.0)
        
        threads = []
        for i, p_batch in enumerate(worker_batches):
            if p_batch:
                t = threading.Thread(target=query_worker, args=(i, p_batch))
                t.start()
                threads.append(t)
        
        for t in threads:
            t.join()
        
        # ============ TIMING: Rollout Complete ============
        rollout_end_time = time.time()
        total_rollout_time = rollout_end_time - rollout_start_time
        
        # 3. Process results
        all_samples = []
        gt_map = {p: a for p, a in zip(prompts, ground_truths)}
        
        for res in worker_results:
            if not res:
                continue
            for item in res["results"]:
                prompt = item["prompt"]
                gt = gt_map.get(prompt)
                
                # Calculate rewards for the group
                rewards = []
                for comp in item["completions"]:
                    r = 1.0 if is_correct(comp, gt) else 0.0
                    rewards.append(r)
                
                print(f"Prompt: {prompt[:50]}... | GT: {gt}")
                print(f"  Rewards: {rewards}")
                print(f"  Sample Output: {item['completions'][0][:100]}...")

                # GRPO: Normalize advantages within group
                rewards_tensor = torch.tensor(rewards, device=self.device, dtype=torch.float32)
                mean_r = rewards_tensor.mean()
                std_r = rewards_tensor.std() + 1e-8
                advantages = (rewards_tensor - mean_r) / std_r
                advantages = torch.clamp(advantages, -3.0, 3.0)
                
                print(f"  Advantages: {advantages.tolist()}")
                
                all_samples.append({
                    "prompt": prompt,
                    "completions": item["completions"],
                    "advantages": advantages
                })

        # 4. Optimization Step
        self.update_model(all_samples)
        
        # Log Step Metrics
        if not all_samples:
            print("Warning: No samples collected this step.")
            return

        step_avg_reward = sum([1.0 if is_correct(c, gt_map.get(s["prompt"])) else 0.0 
                              for s in all_samples for c in s["completions"]]) / (len(all_samples) * config.GRPO_GROUP_SIZE)
        
        # Calculate rollout data size and network metrics
        total_rollout_bytes = sum(rollout_sizes)
        
        # Calculate pure network transmission metrics
        valid_network_times = [t for t in network_times if t is not None]
        avg_network_time = sum(valid_network_times) / len(valid_network_times) if valid_network_times else 0
        
        # Calculate network throughput in Gbps
        if avg_network_time > 0:
            avg_worker_bytes = total_rollout_bytes / len(valid_network_times) if valid_network_times else 0
            network_throughput_gbps = (avg_worker_bytes * 8 / 1e9) / avg_network_time
        else:
            network_throughput_gbps = 0
        
        print(f"\n{'='*60}")
        print(f"ROLLOUT DATA TIMING (Workers → Trainer):")
        print(f"  Total pipeline time:           {total_rollout_time:.3f}s (incl. generation)")
        for i, t in enumerate(rollout_times):
            if t is not None:
                net_str = f" (network: {network_times[i]*1000:.1f}ms)" if network_times[i] else ""
                print(f"    Worker {i}:                      {t:.3f}s{net_str}")
        print(f"  Total rollout data size:       {total_rollout_bytes/1024:.2f} KB")
        print(f"  Avg network transmission:      {avg_network_time*1000:.1f}ms")
        print(f"  Network throughput:            {network_throughput_gbps:.3f} Gbps")
        print(f"{'='*60}\n")
        
        print(f"Step Metrics | Avg Reward: {step_avg_reward:.4f}")
        
        return {
            'weight_broadcast': weight_metrics,
            'rollout_time': total_rollout_time,
            'rollout_size_bytes': total_rollout_bytes,
            'avg_reward': step_avg_reward
        }

    def update_model(self, samples):
        """Update policy model using collected samples"""
        self.policy_model.train()
        
        input_ids_list = []
        attention_mask_list = []
        labels_list = []
        advantages_list = []
        
        for sample in samples:
            prompt = sample["prompt"]
            for i, completion in enumerate(sample["completions"]):
                full_text = f"{prompt} {completion}"
                
                # Encode
                enc = self.tokenizer(full_text, return_tensors="pt", truncation=True, max_length=config.MAX_SEQ_LEN)
                input_ids = enc.input_ids.to(self.device)
                mask = enc.attention_mask.to(self.device)
                
                # Create labels: ignore prompt part
                prompt_enc = self.tokenizer(prompt, return_tensors="pt")
                prompt_len = prompt_enc.input_ids.shape[1]
                
                labels = input_ids.clone()
                if prompt_len < labels.shape[1]:
                    labels[:, :prompt_len] = -100
                else:
                    continue

                input_ids_list.append(input_ids)
                attention_mask_list.append(mask)
                labels_list.append(labels)
                advantages_list.append(sample["advantages"][i])

        if not input_ids_list:
            return

        self.optimizer.zero_grad()
        
        total_loss = 0
        count = 0
        
        for i in range(len(input_ids_list)):
            input_ids = input_ids_list[i]
            attention_mask = attention_mask_list[i]
            labels = labels_list[i]
            adv = advantages_list[i]
            
            # Forward Policy
            outputs = self.policy_model(input_ids=input_ids, attention_mask=attention_mask)
            logits = outputs.logits
            
            # Calculate Logprobs
            shift_logits = logits[..., :-1, :].contiguous()
            shift_labels = labels[..., 1:].contiguous()
            
            log_probs = -F.cross_entropy(shift_logits.view(-1, shift_logits.size(-1)), 
                                        shift_labels.view(-1), 
                                        reduction='none')
            
            log_probs = log_probs.view(shift_labels.shape)
            
            valid_mask = (shift_labels != -100)
            token_log_probs = log_probs * valid_mask
            seq_log_prob = token_log_probs.sum()
            
            # Reference Logprobs (KL)
            with torch.no_grad():
                ref_outputs = self.ref_model(input_ids=input_ids, attention_mask=attention_mask)
                ref_logits = ref_outputs.logits
                ref_shift_logits = ref_logits[..., :-1, :].contiguous()
                
                ref_log_probs_all = -F.cross_entropy(ref_shift_logits.view(-1, ref_shift_logits.size(-1)), 
                                                    shift_labels.view(-1), reduction='none').view(shift_labels.shape)
                ref_token_log_probs = ref_log_probs_all * valid_mask
                ref_seq_log_prob = ref_token_log_probs.sum()
            
            ratio = torch.exp(seq_log_prob - seq_log_prob.detach())
            kl = seq_log_prob - ref_seq_log_prob
            
            loss = -(adv * ratio)
            
            loss.backward()
            total_loss += loss.item()
            count += 1
            
        # Gradient Clipping
        torch.nn.utils.clip_grad_norm_(self.policy_model.parameters(), max_norm=1.0)
        
        # Check for NaNs in grads
        for param in self.policy_model.parameters():
            if param.grad is not None:
                if torch.isnan(param.grad).any() or torch.isinf(param.grad).any():
                    print("Warning: NaN/Inf gradients detected! Skipping step.")
                    self.optimizer.zero_grad()
                    return

        self.optimizer.step()
        
        # Verify weights are finite
        for param in self.policy_model.parameters():
            if torch.isnan(param).any() or torch.isinf(param).any():
                 print("CRITICAL: Model weights became NaN/Inf after step!")
                 exit(1)

        print(f"Batch update complete. Avg Loss: {total_loss/count if count else 0}")

    def run(self):
        """Main training loop"""
        print("Trainer started.")
        self.accept_workers(len(self.worker_connections))
        
        for step in range(config.TRAIN_STEPS):
            print(f"Step {step+1}/{config.TRAIN_STEPS}")
            batch = self.dataset.get_batch(config.BATCH_SIZE)
            self.train_step(batch)

        print("Training complete. Shutting down workers...")
        for i in range(len(self.worker_connections)):
            try:
                self.send_to_worker(i, {"type": "SHUTDOWN"})
            except:
                pass
        time.sleep(1)

        print("Saving model...")
        self.policy_model.save_pretrained("output/qwen-gsm8k-rl")
        self.tokenizer.save_pretrained("output/qwen-gsm8k-rl")
        print("Model saved to output/qwen-gsm8k-rl")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, help="Path to trainer config file")
    args = parser.parse_args()
    
    trainer = Trainer(config_path=args.config)
    trainer.run()
