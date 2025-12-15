import pickle
import time
import io
import tempfile
from pathlib import Path
import torch
import asyncio
from transformers import AutoModelForCausalLM, AutoTokenizer
from . import config

try:
    import nextmini_py as nm
except ImportError as exc:
    raise SystemExit(
        "nextmini_py is not installed. Build the wheel with:\n"
        "  maturin build --release -m python-api/Cargo.toml\n"
        "  pip install target/wheels/nextmini_py-*.whl"
    ) from exc


class Worker:
    def __init__(self, rank: int, device_id: int = 0, config_path: str = None):
        self.rank = rank
        self.device = f"cuda:{device_id}" if torch.cuda.is_available() else "cpu"
        
        print(f"Worker {rank} initializing on {self.device}...")
        
        self.tokenizer = AutoTokenizer.from_pretrained(config.MODEL_NAME, trust_remote_code=True)
        self.model = AutoModelForCausalLM.from_pretrained(
            config.MODEL_NAME, 
            dtype=torch.float16,
            trust_remote_code=True
        ).to(self.device)
        self.model.eval()
        
        # Get worker configuration
        if config_path:
            worker_config = config_path
        else:
            worker_config = config.WORKER_CONFIGS[rank]
            
        local_node_id = config.WORKER_NODE_IDS[rank]
        trainer_node_id = config.TRAINER_NODE_ID
        local_port = config.WORKER_BASE_PORT + rank
        trainer_port = config.TRAINER_PORT
        
        print(f"Worker {rank} config: {worker_config}", flush=True)
        print(f"Worker {rank} node ID: {local_node_id}", flush=True)
        print(f"Trainer node ID: {trainer_node_id}", flush=True)
        print(f"Local port: {local_port}, Trainer port: {trainer_port}", flush=True)
        
        # Initialize nextmini dataplane
        print(f"Initializing nextmini dataplane...", flush=True)
        self.dataplane = nm.Dataplane(worker_config)
        
        # Register receiver for trainer messages
        print(f"Registering receiver for trainer...", flush=True)
        self.receiver = self.dataplane.register_receiver_from_node(
            src_node_id=trainer_node_id,
            src_port=trainer_port,
            dst_port=local_port,
        )
        
        self.trainer_node_id = trainer_node_id
        self.local_port = local_port
        self.trainer_port = trainer_port
        self.trainer_user_ip = None
        
        # Wait for routes to be established
        print(f"Waiting for routes to be established...", flush=True)
        time.sleep(3)  # Trainer's healthcheck ensures it's ready before Workers start
        
        print(f"Worker {rank} ready.", flush=True)

    def send_to_trainer(self, data: dict):
        """Send message to trainer"""
        serialized = pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL)
        view = nm.PacketView(serialized)
        
        self.dataplane.send_to_node(
            dst_node_id=self.trainer_node_id,
            frozen=view,
            src_port=self.local_port,
            dst_port=self.trainer_port,
        )
    
    async def recv_from_trainer(self):
        """Receive message from trainer (async)"""
        delivery = await self.receiver.recv_async()
        
        if delivery is None:
            return None
        
        return pickle.loads(delivery.payload)

    async def _receive_shard(self, group_ip: str, src_node_id: int, expected_bytes: int) -> bytes:
        """Receive a single shard via reliable multicast.
        
        Args:
            group_ip: Multicast group IP
            src_node_id: Source node ID (trainer)
            expected_bytes: Expected size of this shard in bytes
        
        Returns:
            Raw bytes of the received shard
        """
        sid = await self.dataplane.receive_data_async(
            group_ip,
            src_node_id,
            expected_bytes=expected_bytes,
            chunk_size=config.CHUNK_SIZE,
            src_port=config.TRAINER_PORT,
            dst_port=config.WORKER_BASE_PORT
        )
        
        ok = await self.dataplane.reliable_wait_async(sid, timeout_ms=config.MULTICAST_TIMEOUT_MS)
        if not ok:
            raise RuntimeError(f"Failed to receive shard from {src_node_id}")
        
        frozen = self.dataplane.get_data_buffer(sid)
        return bytes(frozen.read())

    async def run(self):
        """Main worker loop"""
        print(f"Worker {self.rank} sending handshake...", flush=True)
        self.send_to_trainer({"type": "HANDSHAKE", "rank": self.rank})
        print(f"Worker {self.rank} handshake sent.", flush=True)
        
        while True:
            msg = await self.recv_from_trainer()
            if not msg:
                print("Trainer disconnected.", flush=True)
                break
            
            if msg["type"] == "HANDSHAKE_ACK":
                # Trainer provides its user-space IP so we can target reliable unicast.
                self.trainer_user_ip = msg.get("trainer_user_ip")
                print(f"Worker {self.rank} received HANDSHAKE_ACK. Trainer user IP: {self.trainer_user_ip}", flush=True)
                continue
            
            if msg["type"] == "WEIGHT_METADATA":
                # Multicast weight synchronization
                print(f"Worker {self.rank} received weight metadata. Preparing for Multicast sync...", flush=True)

                group_id = msg["group_id"]
                group_ip = msg["group_ip"]
                size = msg["size"]
                src_node_id = msg["src_node_id"]
                session_id = msg.get("session_id")

                if not isinstance(session_id, int) or session_id <= 0:
                    print(f"Worker {self.rank}: invalid or missing session_id in WEIGHT_METADATA: {msg}", flush=True)
                    continue

                print(f"Worker {self.rank} metadata: group_id={group_id}, group_ip={group_ip}, size={size} bytes, session_id={session_id}", flush=True)

                # Join the multicast group
                print(f"Worker {self.rank} joining multicast group {group_id}...", flush=True)
                self.dataplane.join_group(group_id)
                print(f"Worker {self.rank} joined group command sent.", flush=True)

                # 2. Register Receive Session in background task (using asyncio)
                print(f"Registering to receive {size} bytes from {src_node_id} (Group {group_id})...", flush=True)

                # Helper to wrap Rust Future into a Python Coroutine for create_task
                async def receive_wrapper():
                    return await self.dataplane.receive_data_async(
                        group_ip,
                        src_node_id,
                        expected_bytes=size,
                        chunk_size=config.CHUNK_SIZE,
                        src_port=config.TRAINER_PORT,
                        dst_port=config.WORKER_BASE_PORT,
                        session_id=session_id,  # Use the same session_id from metadata
                    )

                # Create the receive task - this will submit the request to Rust but won't block
                # until we await it. It returns the Session ID once the first packet arrives.
                receive_task = asyncio.create_task(receive_wrapper())
                
                print(f"Worker {self.rank} receive task created.", flush=True)
                
                # 3. Reply READY (safe to send immediately)
                print(f"Worker {self.rank} sending READY_FOR_MULTICAST...", flush=True)
                self.send_to_trainer({"type": "READY_FOR_MULTICAST"})
                print(f"Worker {self.rank} sent READY_FOR_MULTICAST.", flush=True)
                
                # 4. Wait for the Session ID (this happens when trainer starts sending)
                try:
                    sid = await receive_task
                    print(f"Worker {self.rank} receive_data session established. SID={sid}", flush=True)
                except Exception as e:
                    print(f"Worker {self.rank} failed to establish session: {e}", flush=True)
                    continue

                # 5. Wait for Reliable Transfer Completion
                print(f"Waiting for reliable multicast transfer...")
                ok = await self.dataplane.reliable_wait_async(sid, timeout_ms=config.MULTICAST_TIMEOUT_MS)
                print(f"Receive completion: {ok}")
                
                if ok:
                    frozen = self.dataplane.get_data_buffer(sid)
                    buffer = io.BytesIO(bytes(frozen.read()))
                    state_dict = torch.load(buffer, map_location=self.device)
                    self.model.load_state_dict(state_dict)
                    print("Weights loaded into model.")
                    
                    # Important: Forget the session so next time we don't reuse the old SID
                    # Since multicast group IP + src_node_id is the key, we must clear it 
                    # to allow the 'pending' receiver logic to discover the NEW session ID (e.g. 2, 3...).
                    self.dataplane.forget_session(group_ip, src_node_id)
            
            elif msg["type"] == "WEIGHT_METADATA_SHARDED":
                # Sharded weight synchronization for large models
                print(f"Worker {self.rank} received SHARDED weight metadata.", flush=True)
                
                group_id = msg["group_id"]
                group_ip = msg["group_ip"]
                num_shards = msg["num_shards"]
                shard_names = msg["shard_names"]
                shard_sizes = msg.get("shard_sizes", {})
                has_index = msg["has_index"]
                total_size = msg["total_size"]
                src_node_id = msg["src_node_id"]
                
                print(f"Worker {self.rank}: Expecting {num_shards} shards, total {total_size/1024/1024:.1f} MB", flush=True)
                
                # Join the multicast group
                self.dataplane.join_group(group_id)
                
                # Signal ready
                self.send_to_trainer({"type": "READY_FOR_SHARDED_MULTICAST"})
                
                # Receive all shards into temp directory
                with tempfile.TemporaryDirectory() as tmpdir:
                    tmpdir = Path(tmpdir)
                    
                    # Receive config.json first (required for from_pretrained)
                    config_size = shard_sizes.get("config.json", 1024 * 1024)  # Default 1MB
                    print(f"Worker {self.rank}: Receiving config.json ({config_size} bytes)...", flush=True)
                    config_data = await self._receive_shard(group_ip, src_node_id, config_size)
                    (tmpdir / "config.json").write_bytes(config_data)
                    self.dataplane.forget_session(group_ip, src_node_id)
                    
                    # Receive index (if exists)
                    if has_index:
                        index_size = shard_sizes.get("index", 1024 * 1024)  # Default 1MB
                        print(f"Worker {self.rank}: Receiving index file ({index_size} bytes)...", flush=True)
                        index_data = await self._receive_shard(group_ip, src_node_id, index_size)
                        (tmpdir / "model.safetensors.index.json").write_bytes(index_data)
                        self.dataplane.forget_session(group_ip, src_node_id)
                    
                    # Receive each shard with precise size
                    for shard_name in shard_names:
                        expected_size = shard_sizes.get(shard_name, 8 * 1024 * 1024 * 1024)  # Default 8GB
                        print(f"Worker {self.rank}: Receiving shard {shard_name} ({expected_size/1024/1024:.1f} MB)...", flush=True)
                        shard_data = await self._receive_shard(group_ip, src_node_id, expected_size)
                        (tmpdir / shard_name).write_bytes(shard_data)
                        print(f"Worker {self.rank}: Saved {shard_name} ({len(shard_data)/1024/1024:.1f} MB)", flush=True)
                        self.dataplane.forget_session(group_ip, src_node_id)
                    
                    # Load model from sharded checkpoint
                    print(f"Worker {self.rank}: Loading model from sharded checkpoint...", flush=True)
                    self.model = AutoModelForCausalLM.from_pretrained(
                        str(tmpdir),
                        torch_dtype=torch.float16,
                        trust_remote_code=True
                    ).to(self.device)
                    self.model.eval()
                    
                print(f"Worker {self.rank}: Sharded weights loaded successfully.", flush=True)
            
            elif msg["type"] == "UPDATE_WEIGHTS":
                # Legacy unicast update (not used anymore)
                print("Received weight update (unicast)...")
                self.model.load_state_dict(msg["state_dict"])
                # Send acknowledgement
                self.send_to_trainer({"type": "ACK_WEIGHTS"})
            
            elif msg["type"] == "SHUTDOWN":
                print("Received shutdown signal. Exiting.")
                break
                
            elif msg["type"] == "ROLLOUT":
                prompts = msg["prompts"]
                
                results = []
                print(f"Generating rollouts for {len(prompts)} prompts...", flush=True)
                
                # Offload blocking generation to thread executor to not block heartbeat/other async tasks
                # (Though here we are just waiting for result anyway, so running inline is okay for now)
                
                for prompt in prompts:
                    inputs = self.tokenizer(prompt, return_tensors="pt").to(self.device)
                    
                    with torch.no_grad():
                        outputs = self.model.generate(
                            **inputs,
                            do_sample=True,
                            max_new_tokens=config.GENERATION_LEN,
                            num_return_sequences=config.GRPO_GROUP_SIZE,
                            temperature=1.0,
                            top_p=0.95,
                            pad_token_id=self.tokenizer.eos_token_id
                        )
                    
                    # Decode
                    prompt_len = inputs.input_ids.shape[1]
                    generated_ids = outputs[:, prompt_len:]
                    texts = self.tokenizer.batch_decode(generated_ids, skip_special_tokens=True)
                    
                    results.append({
                        "prompt": prompt,
                        "completions": texts
                    })
                
                # Serialize rollout results
                serialized = pickle.dumps({
                    "type": "ROLLOUT_RESULT",
                    "results": results,
                }, protocol=pickle.HIGHEST_PROTOCOL)
                size = len(serialized)

                # Generate a unique session_id for this transfer
                # Encode node_id in the upper bits for uniqueness across nodes
                local_seq = random.randint(1, 0x0000_FFFF_FFFF_FFFF)
                node_part = (self.dataplane.node_id & 0x7FFF) << 48
                session_id = node_part | local_seq

                # 1) Send small metadata so trainer can allocate receiver
                self.send_to_trainer({
                    "type": "ROLLOUT_METADATA",
                    "size": size,
                    "session_id": session_id,  # Include session_id for deterministic matching
                })

                # 2) Send data reliably via ReliableRuntime using trainer user-space IP
                if not self.trainer_user_ip:
                    print(f"Worker {self.rank}: trainer_user_ip not set, cannot send reliable rollout.", flush=True)
                    continue

                view = nm.PacketView(serialized)
                try:
                    sid = self.dataplane.send_data(
                        self.trainer_user_ip,
                        [self.trainer_node_id],
                        view,
                        chunk_size=config.CHUNK_SIZE,
                        src_port=self.local_port,
                        dst_port=self.trainer_port,
                        session_id=session_id,  # Use the same session_id
                    )
                    ok = await self.dataplane.reliable_wait_async(sid, timeout_ms=config.MULTICAST_TIMEOUT_MS)
                except Exception as e:
                    print(f"Worker {self.rank}: error sending reliable rollout data: {e}", flush=True)
                    continue

                # 3) Send a tiny control message with timestamp for network timing
                send_time = time.time()
                self.send_to_trainer({
                    "type": "ROLLOUT_RESULT",
                    "results": [],
                    "send_timestamp": send_time,
                    "reliable_ok": ok,
                })


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--rank", type=int, default=0)
    parser.add_argument("--gpu", type=int, default=0)
    parser.add_argument("--config", type=str, help="Path to worker config file")
    args = parser.parse_args()
    
    worker = Worker(rank=args.rank, device_id=args.gpu, config_path=args.config)
    asyncio.run(worker.run())
