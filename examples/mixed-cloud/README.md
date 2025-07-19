Test record:

Take London VM as the separate instance for controller and database. 

# Pre-installed tools:

Firstly install docker compose use the following command:

```bash
COMPOSE_VERSION=$(curl -s https://api.github.com/repos/docker/compose/releases/latest | grep 'tag_name' | cut -d\" -f4) && DOCKER_CONFIG=${DOCKER_CONFIG:-$HOME/.docker} && mkdir -p $DOCKER_CONFIG/cli-plugins && curl -L "https://github.com/docker/compose/releases/download/${COMPOSE_VERSION}/docker-compose-$(uname -s)-$(uname -m)" -o $DOCKER_CONFIG/cli-plugins/docker-compose && chmod +x $DOCKER_CONFIG/cli-plugins/docker-compose
```

Checked if the docker compose is installed successfully, use the following command:

```bash
docker compose version
```

Then to run `ifconfig`, you need to install this.

```bash
# for ifconfig
apt install net-tools
```

# Setup the controller and db

At London VM(for controller and database),

## Step 1.1: Gets the ip addr
```bash
cd nextmini/examples/mixed-cloud
```

To get your public IP:

```bash
curl -s ifconfig.me
# Note this IP - we'll need it later.
```

## Step 1.2: Build Controller Image

```bash
docker build -t nextmini_controller -f ../../controller/Dockerfile ../../
```

## Step 1.3: Start Controller and PostgreSQL

```bash
docker compose -f controller-standalone.yml up -d
```

Then use 
```bash
docker ps -a
``` 
to see if controller and postgres are created successfully. 

# Set up the dataplane nodes in manager and worker instance 

## Step 2.1: To start the docker swarm
Then, take the Atlantic DC VM instance as the worker nodes for docker swarm.
```bash
docker swarm init --advertise-addr 129.212.176.245
# This is the real ip addr of Atlantic DC VM(129.212.176.245).
```

On each DigitalOcean VM:
```bash
# Use the token from step 2.1
docker swarm join --token SWMTKN-1-xxxx <SWARM_MANAGER_IP>:2377
```

Then you don't need to do anything in the worker instances until the instances were deployed in 

In Altantic VM instance, open a new terminal, and run:

```bash
# Edit dataplane-swarm.yml to replace IP placeholder, where 167.99.205.149 is the real ip addr of our London DC.
sed 's/REPLACE_WITH_MANAGER_IP/167.99.205.149/g' dataplane-swarm.yml > dataplane-deploy.yml

# Verify the replacement worked
grep CONTROLLER_HOST dataplane-deploy.yml
```

Use the above command you could see a new file named `dataplane-deploy.yml` generated. 

Then on the manager instance, which is the Atlanta instance in our experiment.

```bash
docker stack deploy -c dataplane-deploy.yml nextmini
```

After this, you could use 
```bash
docker service logs nextmini_dataplane
```
to see the loggging info. 

**Note: Don't rush - wait for all nodes to be connected before proceeding with network testing like iperf.**

### iperf test

TODO: To be updated.

## Clean up

To clean up the dataplane worker nodes: use the command below:

```bash
docker stack rm nextmini
```

To clean up the controller & db VM instance, use the command:
```bash
docker compose -f controller-standalone.yml down
```