# sync_github.py
import os
import subprocess
from datetime import datetime

GIT_REMOTE = "https://github.com/gokulmuthuR/crypto-ml-pipeline.git"
BRANCH = "main"

# Folders to keep locally (excluded from clean)
EXCLUDED = ["models", "logs", "tmp", "data/tmp"]

def run(cmd, check=True, safe=False):
    print(f"→ {cmd}")
    res = subprocess.run(cmd, shell=True)
    if check and res.returncode != 0:
        if safe:
            print(f"⚠️ (safe) command failed: {cmd}")
            return res.returncode
        raise SystemExit(f"Command failed: {cmd}")
    return res.returncode

def ensure_repo():
    if not os.path.exists(".git"):
        print("🧩 Initializing git and setting remote...")
        run("git init")
        run(f"git remote add origin {GIT_REMOTE}", safe=True)
        # ensure branch exists locally (safe)
        run(f"git fetch origin {BRANCH}", safe=True)
        run(f"git checkout -b {BRANCH} || git checkout {BRANCH}", safe=True)

def protect_local_files():
    # no-op for files; for directories we will exclude during clean using -e
    pass

def sync():
    print("🔄 Starting clean sync from GitHub...")
    ensure_repo()

    # mark local config files as unchanged so reset won't alter them (if desired)
    # If you want to protect other individual files, add them here (optional)
    local_protect_files = [".replit", "requirements.txt", "README.md"]
    for f in local_protect_files:
        if os.path.exists(f):
            run(f"git update-index --assume-unchanged {f}", check=False, safe=True)

    # fetch and reset
    run(f"git fetch origin {BRANCH}")
    run(f"git reset --hard origin/{BRANCH}")

    # git clean: remove untracked files except EXCLUDED
    # build -e arguments
    exclude_args = " ".join([f"-e {d}" for d in EXCLUDED])
    run(f"git clean -fdx {exclude_args}", safe=True)

    # restore index flags on protected files
    for f in local_protect_files:
        if os.path.exists(f):
            run(f"git update-index --no-assume-unchanged {f}", check=False, safe=True)

    # write last_sync.log for monitoring
    with open("last_sync.log", "w") as fh:
        fh.write(f"Last successful sync: {datetime.utcnow().isoformat()}Z\n")

    print(f"✅ Sync complete at {datetime.utcnow().isoformat()}Z")

if __name__ == "__main__":
    sync()
