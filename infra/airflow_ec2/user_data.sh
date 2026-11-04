#!/bin/bash
set -eux
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y docker.io docker-compose-v2 unzip rsync
systemctl enable --now docker
usermod -aG docker ubuntu
snap install aws-cli --classic || true
mkdir -p /home/ubuntu/airflow
chown ubuntu:ubuntu /home/ubuntu/airflow
touch /home/ubuntu/.setup_done
