terraform {
  required_providers {
    digitalocean = {
      source  = "digitalocean/digitalocean"
      version = "~> 2.0"
    }
  }
}

variable "do_token" {}
variable "id_rsa" {}

provider "digitalocean" {
  token = var.do_token
}

data "digitalocean_ssh_key" "terraform"{
  name="terraform"
}

locals {
  droplet_config = jsondecode(file("${path.module}/droplets.json"))
}

resource "digitalocean_droplet" "droplet" {
  for_each = { for droplet in local.droplet_config : droplet.name => droplet }

  name   = each.value.name
  size   = each.value.size
  image  = each.value.image
  region = each.value.region

  ssh_keys = [
    data.digitalocean_ssh_key.terraform.id
  ]
  connection{
    host = self.ipv4_address
    user = "root"
    type = "ssh"
    private_key = file(var.id_rsa)
    timeout= "2m"
  }

  provisioner "remote-exec"{
    inline = each.value.launch_script
  }
}
