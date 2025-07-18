## How to install mininet

This guide is done on Ubuntu 22.04.

**Step 1**

Clone the git repository of Mininet:

```bash
git clone https://github.com/mininet/mininet
```

Checkout to the recommended version:

```bash
cd mininet; git checkout -b mininet-2.3.0 2.3.0；cd ..
```

**Step 2**

Install all dependencies with:

```bash
mininet/util/install.sh -a
```

If installation failed with the original shell script, you can switch to the `install.sh` provided by in this `/mininet-installation-guide` folder.

**Step 3**

Check for successful installation with:

```bash
sudo mn --version
```

You should see 2.3.0 logged out as selected in the previous step.
