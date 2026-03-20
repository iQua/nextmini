Local SSH credentials for the PyTorch example live in this directory but are not tracked.

Generate a key pair locally and copy the public key into `authorized_keys` before launching the stack:

```bash
ssh-keygen -t rsa -b 4096 -f id_rsa -N ""
cp id_rsa.pub authorized_keys
```
