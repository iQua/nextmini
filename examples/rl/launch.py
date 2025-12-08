import subprocess
import time
import sys
import os

def main():
    # Paths
    base_dir = os.path.dirname(os.path.abspath(__file__))
    src_dir = os.path.join(base_dir, "")
    
    env = os.environ.copy()
    env["PYTHONPATH"] = base_dir
    
    processes = []
    
    print("Starting Trainer...")
    # Start Trainer
    env_trainer = env.copy()
    env_trainer["CUDA_VISIBLE_DEVICES"] = "0"
    p_trainer = subprocess.Popen(
        [sys.executable, "-m", "src.trainer"],
        cwd=base_dir,
        env=env_trainer
    )
    processes.append(p_trainer)
    
    time.sleep(5) # Give trainer time to bind socket
    
    print("Starting Workers...")
    # Start Worker 0
    env_w0 = env.copy()
    env_w0["CUDA_VISIBLE_DEVICES"] = "1"
    p_w0 = subprocess.Popen(
        [sys.executable, "-m", "src.worker", "--rank", "0", "--gpu", "0"],
        cwd=base_dir,
        env=env_w0
    )
    processes.append(p_w0)
    
    # Start Worker 1
    env_w1 = env.copy()
    env_w1["CUDA_VISIBLE_DEVICES"] = "2"
    p_w1 = subprocess.Popen(
        [sys.executable, "-m", "src.worker", "--rank", "1", "--gpu", "0"],
        cwd=base_dir,
        env=env_w1
    )
    processes.append(p_w1)
    
    try:
        while True:
            all_dead = True
            for p in processes:
                ret = p.poll()
                if ret is None:
                    all_dead = False
                elif ret != 0:
                    print(f"Process {p.args} failed with exit code {ret}. Terminating all...")
                    raise KeyboardInterrupt # Trigger cleanup
            
            if all_dead:
                break
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("Stopping all processes...")
        for p in processes:
            if p.poll() is None:
                p.terminate()

if __name__ == "__main__":
    main()
