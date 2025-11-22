import pickle
import time
import torch
import torch.nn.functional as F
from torch.optim import AdamW
from transformers import AutoModelForCausalLM, AutoTokenizer
from . import config
from .dataset import GSM8KLoader, is_correct
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


class Trainer:
    def __init__(self, config_path=None):
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        print(f"Trainer initializing on {self.device}...")
        
        # Load Models
        self.policy_model = AutoModelForCausalLM.from_pretrained(
            config.MODEL_NAME, 
            dtype=torch.float32,
            trust_remote_code=True
        ).to(self.device)
        
        # Reference model (frozen)
        self.ref_model = AutoModelForCausalLM.from_pretrained(
            config.MODEL_NAME, 
            dtype=torch.float32,
            trust_remote_code=True
        ).to(self.device)
        self.ref_model.eval()
        
        self.tokenizer = AutoTokenizer.from_pretrained(config.MODEL_NAME, trust_remote_code=True)
        self.tokenizer.pad_token = self.tokenizer.eos_token
        
        self.optimizer = AdamW(self.policy_model.parameters(), lr=config.LEARNING_RATE)
        
        self.dataset = GSM8KLoader("train")
        
        # Initialize nextmini dataplane
        self.config_path = config_path if config_path else config.TRAINER_CONFIG
        print(f"Initializing nextmini dataplane with config: {self.config_path}")
        self.dataplane = nm.Dataplane(self.config_path)
        
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
        
        # Wait for routes to be established
        print("Waiting for routes to be established...")
        time.sleep(10)
        
        print(f"Trainer ready with {len(self.worker_connections)} workers")

    def accept_workers(self, num_workers=2):
        """Wait for handshake from all workers"""
        print(f"Waiting for {num_workers} workers to send handshake...")
        
        for i, conn in enumerate(self.worker_connections):
            print(f"Waiting for handshake from worker {i}...")
            msg = conn['receiver'].recv(timeout_ms=60000)
            
            if msg is None:
                raise RuntimeError(f"Timeout waiting for handshake from worker {i}")
            
            # Deserialize message
            data = pickle.loads(msg.payload)
            
            if data and data.get("type") == "HANDSHAKE":
                rank = data.get("rank")
                print(f"Worker rank {rank} (node {conn['node_id']}) identified")
            else:
                raise RuntimeError(f"Invalid handshake from worker {i}: {data}")
        
        print("All workers connected.")

    def send_to_worker(self, worker_idx: int, data: dict):
        """Send message to specific worker"""
        conn = self.worker_connections[worker_idx]
        serialized = pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL)
        frozen = nm.FrozenBuffer(serialized)
        
        self.dataplane.send_to_node(
            dst_node_id=conn['node_id'],
            frozen=frozen,
            src_port=config.TRAINER_PORT,
            dst_port=conn['port'],
        )
    
    def recv_from_worker(self, worker_idx: int, timeout_ms: int = 30000):
        """Receive message from specific worker"""
        conn = self.worker_connections[worker_idx]
        delivery = conn['receiver'].recv(timeout_ms=timeout_ms)
        
        if delivery is None:
            return None
        
        return pickle.loads(delivery.payload)

    def broadcast_weights(self):
        """Broadcast model weights to all workers"""
        print("Broadcasting weights to workers...")
        state_dict = self.policy_model.state_dict()
        # Move to CPU for serialization
        state_dict_cpu = {k: v.cpu() for k, v in state_dict.items()}
        
        def send_to_worker_thread(i):
            with self.worker_locks[i]:
                self.send_to_worker(i, {"type": "UPDATE_WEIGHTS", "state_dict": state_dict_cpu})
                # Wait for ACK
                self.recv_from_worker(i)
        
        threads = []
        for i in range(len(self.worker_connections)):
            t = threading.Thread(target=send_to_worker_thread, args=(i,))
            t.start()
            threads.append(t)
        
        for t in threads:
            t.join()
        
        print("Weights synced.")

    def train_step(self, batch):
        """Execute one training step"""
        # 1. Broadcast current weights
        self.broadcast_weights()
        
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
        
        def query_worker(i, p_batch):
            with self.worker_locks[i]:
                self.send_to_worker(i, {"type": "ROLLOUT", "prompts": p_batch})
                worker_results[i] = self.recv_from_worker(i)
        
        threads = []
        for i, p_batch in enumerate(worker_batches):
            if p_batch:
                t = threading.Thread(target=query_worker, args=(i, p_batch))
                t.start()
                threads.append(t)
        
        for t in threads:
            t.join()
        
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
        print(f"Step Metrics | Avg Reward: {step_avg_reward:.4f}")

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
        self.accept_workers()
        
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
