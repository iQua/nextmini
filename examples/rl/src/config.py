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
_worker_node_ids_env = os.environ.get("WORKER_NODE_IDS")
if _worker_node_ids_env:
    WORKER_NODE_IDS = [
        int(part.strip())
        for part in _worker_node_ids_env.split(",")
        if part.strip()
    ]

# Communication ports
TRAINER_PORT = 5000
WORKER_BASE_PORT = 5001  # Workers use BASE_PORT + rank

# Multicast
MULTICAST_GROUP_NAME = "rl_weights_sync"
MULTICAST_TIMEOUT_MS = 60000 # 60s timeout for large weights
CHUNK_SIZE = 8500 # Standard chunk size
MAX_IN_MEMORY_RECEIVE_BYTES = int(
    os.environ.get("MAX_IN_MEMORY_RECEIVE_BYTES", str(512 * 1024 * 1024))
)
ROLLOUT_GROUP_ID_BASE = int(os.environ.get("ROLLOUT_GROUP_ID_BASE", "1000"))

# Multicast planning (LP / CF-Tree)
MULTICAST_TREE_ALGO = os.environ.get("MULTICAST_TREE_ALGO", "cf_tree")
MULTICAST_HOP_LIMIT = int(os.environ.get("MULTICAST_HOP_LIMIT", "3"))
MULTICAST_ETA = float(os.environ.get("MULTICAST_ETA", "0.1"))
MULTICAST_RELAY_SCORING = os.environ.get("MULTICAST_RELAY_SCORING", "coverage")
MULTICAST_MAX_RELAYS = os.environ.get("MULTICAST_MAX_RELAYS")
MULTICAST_MAX_RELAYS_INT = (
    int(MULTICAST_MAX_RELAYS) if MULTICAST_MAX_RELAYS not in (None, "") else None
)
MULTICAST_NUM_PATHS = int(os.environ.get("MULTICAST_NUM_PATHS", "2"))
MULTICAST_ALLOW_WORKER_RELAYS = os.environ.get(
    "MULTICAST_ALLOW_WORKER_RELAYS", "false"
).lower() == "true"

# Optional: probe link goodputs via controller DB before planning.
MULTICAST_PROBE_LINKS = os.environ.get("MULTICAST_PROBE_LINKS", "false").lower() == "true"
MULTICAST_PROBE_BYTES = int(os.environ.get("MULTICAST_PROBE_BYTES", str(64 * 1024 * 1024)))
MULTICAST_PROBE_TIMEOUT_SECS = float(os.environ.get("MULTICAST_PROBE_TIMEOUT_SECS", "60"))
MULTICAST_PROBE_BATCH_SIZE = int(os.environ.get("MULTICAST_PROBE_BATCH_SIZE", "0"))


def rollout_group_id(node_id: int) -> int:
    return ROLLOUT_GROUP_ID_BASE + int(node_id)

# Sharded Checkpoint (for large models)
USE_SHARDED_WEIGHTS = os.environ.get("USE_SHARDED_WEIGHTS", "false").lower() == "true"
SHARD_SIZE = os.environ.get("SHARD_SIZE", "8GB")  # Default shard size for large models

# Model
MODEL_NAME = os.environ.get("MODEL_NAME", "Qwen/Qwen2.5-0.5B-Instruct")
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
