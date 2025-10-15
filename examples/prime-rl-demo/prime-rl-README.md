Set up PRIME-RL in one command:
```bash
curl -sSL https://raw.githubusercontent.com/PrimeIntellect-ai/prime-rl/main/scripts/install.sh | bash
```

Login to Hugging Face:
```bash
uv run hf auth login
```

SFT on 2 GPUs (and save weights):
```bash
uv run torchrun \
  --nproc-per-node=2 \
  src/prime_rl/trainer/sft/train.py @ examples/reverse_text/sft/train.toml \
  --weights
```

Upload the SFT model to Hugging Face:
```bash
cd /home/xindan/prime-rl && huggingface-cli upload Winifredkkkk/Qwen3-0.6B-Reverse-Text-SFT outputs/weights/step_100 --repo-type model
```

If a previous vLLM process is running, stop it first:
```bash
pkill -f "VLLM::EngineCore"
```

<!-- Then run:

```bash
uv run rl   --trainer @ examples/reverse_text/rl/train.toml   --orchestrator @ examples/reverse_text/rl/orch.toml   --inference @ examples/reverse_text/rl/infer.toml   --model.name Winifredkkkk/Qwen3-0.6B-Reverse-Text-SFT
``` -->

Watch GPU usage (optional):
```bash
watch -n 1 nvidia-smi
```

Run RL (inference on GPU 0, trainer on GPU 1 and 2). NCCL envs avoid SHM/P2P issues:
```bash
CUDA_VISIBLE_DEVICES=0,1,2 \
NCCL_IB_DISABLE=1 NCCL_SHM_DISABLE=1 NCCL_P2P_DISABLE=1 NCCL_NET_GDR_LEVEL=0 NCCL_COLLNET_ENABLE=0 \
uv run rl \
  --trainer @ examples/reverse_text/rl/train.toml \
  --orchestrator @ examples/reverse_text/rl/orch.toml \
  --inference @ examples/reverse_text/rl/infer.toml \
  --model.name Winifredkkkk/Qwen3-0.6B-Reverse-Text-SFT \
  --inference-gpu-ids "[0]" \
  --trainer-gpu-ids "[1, 2]"
```

This command can successfully run `prime-rl/examples/reverse` example in Sim.

Checking HF account (optional):
```bash
cd /home/xindan/prime-rl && huggingface-cli whoami
```

Upload the RL model to Hugging Face (after training completes):
```bash
cd /home/xindan/prime-rl && huggingface-cli upload Winifredkkkk/Qwen3-0.6B-Reverse-Text-RL outputs/weights/step_20 --repo-type model
```

Evaluate the RL model
1) Start inference:
```bash
uv run inference --model.name Winifredkkkk/Qwen3-0.6B-Reverse-Text-RL
```
2) In another terminal, run eval:
```bash
uv run vf-eval reverse-text -m Winifredkkkk/Qwen3-0.6B-Reverse-Text-RL -b http://localhost:8000/v1 -n 20 --max-tokens 1024
```

Sample result:
```text
--- All ---
Rewards:
reward: avg - 0.863, std - 0.059
```
