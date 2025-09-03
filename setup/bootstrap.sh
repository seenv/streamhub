#!/bin/bash
set -e

# Install Ansible if missing
if ! command -v ansible &> /dev/null; then
  echo "[INFO] Installing Ansible..."
  sudo apt update
  sudo apt install -y software-properties-common
  sudo add-apt-repository --yes --update ppa:ansible/ansible
  sudo apt install -y ansible
fi

# Run playbook
#ansible-playbook site.yml -i inventory/hosts --ask-pass
ansible-playbook site.yml -i inventory/hosts
