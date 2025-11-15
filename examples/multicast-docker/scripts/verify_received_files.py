"""
uv run examples/multicast-docker/scripts/verify_received_files.py
"""

import hashlib
import sys
from pathlib import Path
from typing import Dict, List

# Fixed configuration
ARTIFACT_DIR = Path(__file__).parent.parent / "artifacts"
FILE_PATTERN = "receiver-*.bin"


def compute_checksum(file_path: Path) -> str:
    hash_obj = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(8192):
            hash_obj.update(chunk)
    return hash_obj.hexdigest()


def verify_files() -> int:
    files = sorted(ARTIFACT_DIR.glob(FILE_PATTERN))
    
    if not files:
        print(f"No files matching '{FILE_PATTERN}' found in {ARTIFACT_DIR}")
        return 1
    
    print(f"Found {len(files)} receiver file(s)")
    print("=" * 70)
    
    checksums: Dict[str, List[str]] = {}
    file_info = []
    
    for file_path in files:
        file_size = file_path.stat().st_size
        print(f"Computing checksum for {file_path.name}... ", end="", flush=True)
        checksum = compute_checksum(file_path)
        print("done")
        
        file_info.append({
            "name": file_path.name,
            "size": file_size,
            "checksum": checksum
        })
        
        if checksum not in checksums:
            checksums[checksum] = []
        checksums[checksum].append(file_path.name)
    
    print("=" * 70)
    print(f"\nTotal files: {len(files)}")
    print(f"Unique checksums: {len(checksums)}")
    print()
    
    # Check if file sizes are consistent
    sizes = set(info["size"] for info in file_info)
    if len(sizes) > 1:
        print("WARNING: File sizes are inconsistent")
        for info in file_info:
            print(f"  {info['name']}: {info['size']:,} bytes")
        print()
    
    # Display results
    if len(checksums) == 1:
        checksum = list(checksums.keys())[0]
        print("VERIFICATION PASSED: All receivers got identical data")
        print(f"\nSHA256 checksum:")
        print(f"  {checksum}")
        print(f"\nVerified files:")
        for filename in checksums[checksum]:
            print(f"  {filename}")
        return 0
    else:
        print("VERIFICATION FAILED: Receivers got different data")
        print(f"\nFound {len(checksums)} different checksum(s):\n")
        for i, (checksum, filenames) in enumerate(checksums.items(), 1):
            print(f"Checksum #{i}:")
            print(f"  SHA256: {checksum}")
            print(f"  File count: {len(filenames)}")
            print(f"  Files:")
            for filename in filenames:
                print(f"    - {filename}")
            print()
        return 1


def main():
    if not ARTIFACT_DIR.exists():
        print(f"Error: Directory {ARTIFACT_DIR} does not exist")
        return 1
    
    if not ARTIFACT_DIR.is_dir():
        print(f"Error: {ARTIFACT_DIR} is not a directory")
        return 1
    
    return verify_files()


if __name__ == "__main__":
    sys.exit(main())
