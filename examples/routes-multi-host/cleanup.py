#!/usr/bin/env python3
"""
Cleanup Nextmini deployment
Usage: uv run cleanup.py
"""

import argparse
import subprocess
import sys
from pathlib import Path


def run_command(cmd, check=False):
    """Run a command and return success status."""
    try:
        result = subprocess.run(cmd, shell=True, check=check,
                              capture_output=True, text=True)
        return result.returncode == 0
    except subprocess.CalledProcessError:
        return False


def main():
    parser = argparse.ArgumentParser(
        description='Cleanup Nextmini deployment',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Cleanup all
  uv run cleanup.py
  
  # Cleanup but keep deployment directories
  uv run cleanup.py --keep-dirs
        """
    )
    
    parser.add_argument(
        '--keep-dirs',
        action='store_true',
        help='Keep deployment directories (only stop processes)'
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("Cleaning up Nextmini deployment")
    print("=" * 60)
    
    # Stop all nextmini processes
    print("\nStopping dataplane nodes...")
    if run_command("sudo pkill -9 -f 'nextmini'"):
        print("✓ Dataplane nodes stopped")
    else:
        print("  No dataplane nodes running")
    
    # Stop controller
    print("\nStopping controller...")
    if run_command("sudo pkill -9 -f 'controller'"):
        print("✓ Controller stopped")
    else:
        print("  No controller running")
    
    # Stop database
    print("\nStopping PostgreSQL database...")
    stop_result = run_command("docker stop nextmini-database")
    rm_result = run_command("docker rm nextmini-database")
    if stop_result or rm_result:
        print("✓ Database stopped and removed")
    else:
        print("  No database container running")
    
    # Remove deployment directories
    if not args.keep_dirs:
        script_dir = Path(__file__).parent.resolve()
        
        dirs_to_remove = []
        dirs_to_remove.append(script_dir / "controller-deploy")
        
        # Find all node deploy directories
        for item in script_dir.iterdir():
            if item.is_dir() and item.name.startswith("node") and item.name.endswith("-deploy"):
                dirs_to_remove.append(item)
        
        if dirs_to_remove:
            print("\nRemoving deployment directories...")
            for dir_path in dirs_to_remove:
                if dir_path.exists():
                    import shutil
                    shutil.rmtree(dir_path)
                    print(f"✓ Removed {dir_path.name}")
        else:
            print("\n  No deployment directories found")
    else:
        print("\n  Keeping deployment directories (--keep-dirs specified)")
    
    print("\n" + "=" * 60)
    print("Cleanup complete")
    print("=" * 60)


if __name__ == "__main__":
    main()

