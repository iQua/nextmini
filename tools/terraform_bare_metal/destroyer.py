import os
import init 
os.chdir(os.path.dirname(__file__))

#Run terraform apply
with open("dataplane/do_token", "r") as f:
    do_token = f.read().strip()
    id_rsa = os.path.abspath("dataplane/id_rsa")
    os.system(f"terraform destroy -var 'do_token={do_token}' -var 'id_rsa={id_rsa}'")

