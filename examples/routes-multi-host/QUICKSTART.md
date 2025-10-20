# Quick Start

## Scenario

- Controller: `206.12.89.244` (Physical Machine 1)
- Node 1: Physical Machine 2
- Node 2: Physical Machine 3

All nodes use port `8080` (no conflict).

---

## Step 1: Build (Once)

```bash
cd nextmini
cargo run -p cert-gen
cargo build --release -p controller
cargo build --release -p nextmini
```

---

## Step 2: Deploy Controller (206.12.89.244)

```bash
cd ~/nextmini
cp -r examples/routes-multi-host ~/deploy
cd ~/deploy
chmod +x *.py
uv run deploy_controller.py
```

---

## Step 3: Deploy Nodes

### Node Machine 1:
```bash
cd ~/nextmini
cp -r examples/routes-multi-host ~/deploy
cd ~/deploy
chmod +x *.py
uv run deploy_node.py --controller-ip 206.12.89.244 --node-id 1
```

### Node Machine 2:
```bash
cd ~/nextmini
cp -r examples/routes-multi-host ~/deploy
cd ~/deploy
chmod +x *.py
uv run deploy_node.py --controller-ip 206.12.89.244 --node-id 2
```

---

## Verify

```bash
# Controller
tail -f ~/deploy/controller-deploy/controller.log

# Nodes
tail -f ~/deploy/node1-deploy/node1.log
tail -f ~/deploy/node2-deploy/node2.log
```

---

## Cleanup

```bash
cd ~/deploy
uv run cleanup.py
```

---

## Help

```bash
uv run deploy_node.py --help
```
