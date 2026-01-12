# Multi-DC WAN inventory (Ubuntu VMs)
#
# Trainer (+ controller/postgres): 1
# Workers (rank 0..6): 2,3,4,5,8,9,11
# Relays: 6,7,10

[ssh]
user = "root"
port = 22
identity_file = "~/.ssh/no-key"

[paths]
remote_repo_dir = "~/nextmini"

[controller]
host = "157.180.84.40"
public_ip = "157.180.84.40"

[[nodes]]
role = "trainer"
node_id = 1
host = "157.180.84.40"
public_ip = "157.180.84.40"
region = "helsinki-finland"

[[nodes]]
role = "worker"
rank = 0
node_id = 2
host = "206.12.92.2"
public_ip = "206.12.92.2"
region = "victoria-canada"
user = "ubuntu"
network_interface = "ens3"


[[nodes]]
role = "worker"
rank = 1
node_id = 3
host = "206.12.89.244"
public_ip = "206.12.89.244"
region = "victoria-canada"
user = "ubuntu"
network_interface = "ens3"

[[nodes]]
role = "worker"
rank = 2
node_id = 4
host = "142.150.238.6"
public_ip = "142.150.238.6"
region = "toronto-canada"
user = "xindan"
network_interface = "eno2"

[[nodes]]
role = "worker"
rank = 3
node_id = 5
host = "167.71.44.237"
public_ip = "167.71.44.237"
region = "frankfurt-germany"
user = "root"

[[nodes]]
role = "relay"
node_id = 6
host = "167.99.73.109"
public_ip = "167.99.73.109"
region = "singapore"
user = "root"

[[nodes]]
role = "relay"
node_id = 7
host = "164.92.84.131"
public_ip = "164.92.84.131"
region = "santa-clara-usa"
user = "root"

[[nodes]]
role = "worker"
rank = 4
node_id = 8
host = "178.128.250.225"
public_ip = "178.128.250.225"
region = "amsterdam-netherlands"
user = "root"

[[nodes]]
role = "relay"
node_id = 9
host = "206.189.17.48"
public_ip = "206.189.17.48"
region = "london-uk"
user = "root"

[[nodes]]
role = "worker"
rank = 5
node_id = 10
host = "139.59.17.249"
public_ip = "139.59.17.249"
region = "bengaluru-india"
user = "root"
network_interface = "eth0"
 