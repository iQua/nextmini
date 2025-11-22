import pickle
import time
import io
import torch
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
        
        print(f"Worker {rank} config: {worker_config}")
        print(f"Worker {rank} node ID: {local_node_id}")
        print(f"Trainer node ID: {trainer_node_id}")
        print(f"Local port: {local_port}, Trainer port: {trainer_port}")
        
        # Initialize nextmini dataplane
        print(f"Initializing nextmini dataplane...")
        self.dataplane = nm.Dataplane(worker_config)
        
        # Register receiver for trainer messages
        print(f"Registering receiver for trainer...")
        self.receiver = self.dataplane.register_receiver_from_node(
            src_node_id=trainer_node_id,
            src_port=trainer_port,
            dst_port=local_port,
        )
        
        self.trainer_node_id = trainer_node_id
        self.local_port = local_port
        self.trainer_port = trainer_port
        
        # Wait for routes
        print(f"Waiting for routes to be established...")
        time.sleep(5)
        
        print(f"Worker {rank} ready.")

    def send_to_trainer(self, data: dict):
        """Send message to trainer"""
        serialized = pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL)
        frozen = nm.FrozenBuffer(serialized)
        
        self.dataplane.send_to_node(
            dst_node_id=self.trainer_node_id,
            frozen=frozen,
            src_port=self.local_port,
            dst_port=self.trainer_port,
        )
    
    def recv_from_trainer(self, timeout_ms: int = 30000):
        """Receive message from trainer"""
        delivery = self.receiver.recv(timeout_ms=timeout_ms)
        
        if delivery is None:
            return None
        
        return pickle.loads(delivery.payload)

    def run(self):
        """Main worker loop"""
        print(f"Worker {self.rank} sending handshake...")
        self.send_to_trainer({"type": "HANDSHAKE", "rank": self.rank})
        print(f"Worker {self.rank} handshake sent.")
        
        while True:
            msg = self.recv_from_trainer()
            if msg is None:
                print("Trainer disconnected.")
                break
            
            if msg["type"] == "WEIGHT_METADATA":
                print("Received weight metadata. Preparing for Multicast sync...")
                group_id = msg["group_id"]
                group_ip = msg["group_ip"]
                size = msg["size"]
                src_node_id = msg["src_node_id"]
                
                # 1. Join Group (idempotent)
                print(f"Joining multicast group {group_id}...")
                self.dataplane.join_group(group_id)
                
                # 2. Register Receive Session FIRST (important for timing)
                print(f"Registering to receive {size} bytes from {src_node_id} (Group {group_id})...")
                sid = self.dataplane.receive_data(
                    group_ip,
                    src_node_id,
                    expected_bytes=size,
                    chunk_size=config.CHUNK_SIZE,
                    src_port=config.TRAINER_PORT,
                    dst_port=config.WORKER_BASE_PORT # Must match trainer's dst_port
                )
                
                # 3. Reply READY (after receive_data is registered)
                self.send_to_trainer({"type": "READY_FOR_MULTICAST"})
                
                # 4. Wait for Reliable Transfer
                print(f"Waiting for reliable multicast transfer...")
                ok = self.dataplane.reliable_wait(sid, timeout_ms=config.MULTICAST_TIMEOUT_MS)
                print(f"Receive completion: {ok}")
                
                if ok:
                    frozen = self.dataplane.get_data_buffer(sid)
                    buffer = io.BytesIO(bytes(frozen.read()))
                    state_dict = torch.load(buffer, map_location=self.device)
                    self.model.load_state_dict(state_dict)
                    print("Weights loaded into model.")
            
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
                
                self.send_to_trainer({"type": "ROLLOUT_RESULT", "results": results})


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--rank", type=int, default=0)
    parser.add_argument("--gpu", type=int, default=0)
    parser.add_argument("--config", type=str, help="Path to worker config file")
    args = parser.parse_args()
    
    worker = Worker(rank=args.rank, device_id=args.gpu, config_path=args.config)
    worker.run()
