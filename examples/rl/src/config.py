import os


def _csv_env(name, default):
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return list(default)
    return [item.strip() for item in raw.split(",") if item.strip()]


def _int_csv_env(name, default):
    return [int(item) for item in _csv_env(name, [str(value) for value in default])]


# Optional: probe link goodputs via controller DB before planning.
MULTICAST_PROBE_LINKS = os.environ.get("MULTICAST_PROBE_LINKS", "false").lower() == "true"
MULTICAST_PROBE_BYTES = int(os.environ.get("MULTICAST_PROBE_BYTES", str(64 * 1024 * 1024)))
MULTICAST_PROBE_TIMEOUT_SECS = float(os.environ.get("MULTICAST_PROBE_TIMEOUT_SECS", "60"))

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
WORKER_CONFIGS = _csv_env("WORKER_CONFIGS", WORKER_CONFIGS)

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
WORKER_NODE_IDS = _int_csv_env("WORKER_NODE_IDS", WORKER_NODE_IDS)

if len(WORKER_CONFIGS) != len(WORKER_NODE_IDS):
    raise ValueError(
        "WORKER_CONFIGS and WORKER_NODE_IDS must have the same length "
        f"({len(WORKER_CONFIGS)} != {len(WORKER_NODE_IDS)})"
    )

# Communication ports
TRAINER_PORT = 5000
WORKER_BASE_PORT = 5001  # Workers use BASE_PORT + rank

# Multicast
MULTICAST_GROUP_NAME = "rl_weights_sync"
MULTICAST_TIMEOUT_MS = int(os.environ.get("MULTICAST_TIMEOUT_MS", "60000"))
CHUNK_SIZE = 8500 # Standard chunk size
ROLLOUT_GROUP_ID_BASE = int(os.environ.get("ROLLOUT_GROUP_ID_BASE", "1000"))


def rollout_group_id(node_id: int) -> int:
    return ROLLOUT_GROUP_ID_BASE + int(node_id)

# Sharded Checkpoint (for large models)
USE_SHARDED_WEIGHTS = os.environ.get("USE_SHARDED_WEIGHTS", "false").lower() == "true"
SHARD_SIZE = os.environ.get("SHARD_SIZE", "8GB")  # Default shard size for large models

# Model
MODEL_NAME = os.environ.get("MODEL_NAME", "Qwen/Qwen2.5-0.5B-Instruct")
MODEL_DTYPE = os.environ.get("MODEL_DTYPE", "auto")
MAX_SEQ_LEN = int(os.environ.get("MAX_SEQ_LEN", "1024"))
GENERATION_LEN = int(os.environ.get("GENERATION_LEN", "256"))

# Training
GRPO_GROUP_SIZE = int(os.environ.get("GRPO_GROUP_SIZE", "4"))  # G in GRPO paper
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "2"))  # prompts per training step
LEARNING_RATE = float(os.environ.get("LEARNING_RATE", "1e-6"))
KL_COEFF = float(os.environ.get("KL_COEFF", "0.01"))
CLIP_EPS = float(os.environ.get("CLIP_EPS", "0.2"))
TRAIN_STEPS = int(os.environ.get("TRAIN_STEPS", "1000"))
BROADCAST_ONLY = os.environ.get("BROADCAST_ONLY", "false").lower() == "true"

# Roles
ROLE_TRAINER = "trainer"
ROLE_WORKER = "worker"

# System
DEVICE_MAP = "auto"  # Let accelerate/transformers handle it
