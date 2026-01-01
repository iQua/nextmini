import os

# Nextmini Node Configuration
# Paths to node config files (TOML)
TRAINER_CONFIG = os.environ.get(
    "TRAINER_CONFIG",
    "configs/trainer-config.toml"
)
WORKER_CONFIGS = [
    os.environ.get("WORKER0_CONFIG", "configs/worker0-config.toml"),
    os.environ.get("WORKER1_CONFIG", "configs/worker1-config.toml"),
]

# Controller config (for LP multicast routing)
CONTROLLER_CONFIG = os.environ.get(
    "CONTROLLER_CONFIG",
    "examples/rl/configs-docker/controller-config.toml",
)

# Node IDs (must match node_id in config files)
TRAINER_NODE_ID = int(os.environ.get("TRAINER_NODE_ID", "1"))
WORKER_NODE_IDS = [
    int(os.environ.get("WORKER0_NODE_ID", "2")),
    int(os.environ.get("WORKER1_NODE_ID", "3")),
]

# Communication ports
TRAINER_PORT = 5000
WORKER_BASE_PORT = 5001  # Workers use BASE_PORT + rank

# Multicast
MULTICAST_GROUP_NAME = "rl_weights_sync"
MULTICAST_TIMEOUT_MS = 60000 # 60s timeout for large weights
CHUNK_SIZE = 8500 # Standard chunk size
ROLLOUT_GROUP_ID_BASE = int(os.environ.get("ROLLOUT_GROUP_ID_BASE", "1000"))


def rollout_group_id(node_id: int) -> int:
    return ROLLOUT_GROUP_ID_BASE + int(node_id)

# Sharded Checkpoint (for large models)
USE_SHARDED_WEIGHTS = os.environ.get("USE_SHARDED_WEIGHTS", "false").lower() == "true"
SHARD_SIZE = os.environ.get("SHARD_SIZE", "8GB")  # Default shard size for large models

# Model
MODEL_NAME = "Qwen/Qwen2.5-0.5B-Instruct"
MAX_SEQ_LEN = 1024
GENERATION_LEN = 256

# Training
GRPO_GROUP_SIZE = 4  # G in GRPO paper (number of samples per prompt)
BATCH_SIZE = 2  # Number of prompts per training step
LEARNING_RATE = 1e-6
KL_COEFF = 0.01
CLIP_EPS = 0.2
TRAIN_STEPS = 1000

# Roles
ROLE_TRAINER = "trainer"
ROLE_WORKER = "worker"

# System
DEVICE_MAP = "auto"  # Let accelerate/transformers handle it
