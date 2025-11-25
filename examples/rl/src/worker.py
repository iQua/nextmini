import pickle
import time
import io
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

    async def multicast_loop(self, group_ip, src_node_id):
        """Background task to listen for multicast weight updates"""
        print(f"Worker {self.rank} starting multicast listener loop...", flush=True)
        while True:
            try:
                # Wait for next session to start (first packet arrival)
                # expected_bytes=0 signals we wait for Manifest to know the size
                sid = await self.dataplane.receive_data_async(
                    group_ip,
                    src_node_id,
                    expected_bytes=0,
                    chunk_size=config.CHUNK_SIZE,
                    src_port=config.TRAINER_PORT,
                    dst_port=config.WORKER_BASE_PORT
                )
                print(f"Worker {self.rank} detected multicast session start. SID={sid}", flush=True)
                
                # Wait for completion
                ok = await self.dataplane.reliable_wait_async(sid, timeout_ms=config.MULTICAST_TIMEOUT_MS)
                
                if ok:
                    frozen = self.dataplane.get_data_buffer(sid)
                    buffer = io.BytesIO(bytes(frozen.read()))
                    state_dict = torch.load(buffer, map_location=self.device)
                    self.model.load_state_dict(state_dict)
                    print(f"Worker {self.rank} weights updated via multicast.", flush=True)
                    
                    # Clean up session to allow next one to be discovered
                    self.dataplane.forget_session(group_ip, src_node_id)
                else:
                    print(f"Worker {self.rank} multicast session timed out/failed.", flush=True)
                    # Don't forget session immediately on timeout? 
                    # If we timed out, maybe we should to avoid getting stuck? 
                    # Yes, safe to forget.
                    self.dataplane.forget_session(group_ip, src_node_id)
                    
            except Exception as e:
                print(f"Worker {self.rank} multicast loop error: {e}", flush=True)
                # Avoid tight loop on error
                await asyncio.sleep(1)

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
            
            if msg["type"] == "GROUP_INFO":
                # One-time setup for multicast
                group_id = msg["group_id"]
                group_ip = msg["group_ip"]
                src_node_id = msg["src_node_id"]
                
                print(f"Worker {self.rank} received GROUP_INFO: {group_id} ({group_ip})", flush=True)
                
                # Join the multicast group
                print(f"Worker {self.rank} joining multicast group {group_id}...", flush=True)
                self.dataplane.join_group(group_id)
                
                # Start background listener
                asyncio.create_task(self.multicast_loop(group_ip, src_node_id))
                
            # elif msg["type"] == "WEIGHT_METADATA":
            #     # Legacy handler - ignore or warn
            #     print(f"Worker {self.rank} received unexpected WEIGHT_METADATA (deprecated)", flush=True)
            
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
                print(f"Generating rollouts for {len(prompts)} prompts...")
                
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
                
                # Add timestamp for network transmission measurement
                send_time = time.time()
                self.send_to_trainer({
                    "type": "ROLLOUT_RESULT",
                    "results": results,
                    "send_timestamp": send_time
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
