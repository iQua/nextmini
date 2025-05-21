import os
import json
import yaml

os.chdir(os.path.dirname(__file__))

configs = {}
with open("dataplane/dataplane.yml", "r") as f:
    configs = yaml.load(f, Loader=yaml.FullLoader)
    
ssh_private_key = ""
with open("dataplane/id_rsa", "r") as f:
    ssh_private_key = f.read()
    
regions = configs["regions"]
droplet_size = configs["droplet_size"]
controller_addr = configs["controller_addr"]
image = "docker-20-04"

public_ip_query = r"$(ifconfig | awk '/eth0:/{flag=1} /RX packets/{flag=0} flag' | grep -oE 'inet [0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' | grep -oE '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+')"
init_script = f'''apt install net-tools -y
STRATO_PUBLIC_ADDR={public_ip_query}
echo export STRATO_PUBLIC_ADDR=$STRATO_PUBLIC_ADDR >> ~/.bashrc
echo export CONTROLLER_ADDR={controller_addr} >> ~/.bashrc
echo "{ssh_private_key}" > ~/.ssh/id_rsa
chmod 400 ~/.ssh/id_rsa
GIT_SSH_COMMAND="ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no" git clone git@github.com:iQua/strato.git
cd ~/strato/examples/dataplane_headless
docker compose up -d'''

def init():
    droplets = []
    node_id = 0
    for region in regions:
        region, number = list(region.keys())[0], list(region.values())[0]
        for i in range(number):
            script = [f"echo export NODE_ID={node_id} >> ~/.bashrc"]
            script.extend(init_script.split("\n"))
            droplet = {
                "name": f"strato-{region}-{i}",
                "size": droplet_size,
                "image": image,
                "region": region,
                "launch_script": script
            }
            node_id += 1
            droplets.append(droplet)

    print(init_script)
    with open("droplets.json", "w") as f:
        json.dump(droplets, f, indent=4)
        
if __name__=="__main__":
    
    init()