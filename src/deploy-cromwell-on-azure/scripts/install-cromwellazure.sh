#!/bin/bash
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

# Install Docker and Docker Compose
sudo apt update
sudo apt install -y apt-transport-https ca-certificates curl software-properties-common
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo add-apt-repository -y "deb [arch=amd64] https://download.docker.com/linux/ubuntu bionic stable"
sudo apt update
sudo apt-cache policy docker-ce
sudo apt install -y docker-ce
sudo curl -L https://github.com/docker/compose/releases/download/1.24.1/docker-compose-`uname -s`-`uname -m` -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
sudo docker-compose --version

# Install Blobfuse
sudo wget https://packages.microsoft.com/config/ubuntu/16.04/packages-microsoft-prod.deb
sudo dpkg -i packages-microsoft-prod.deb
sudo apt update
sudo apt install -y blobfuse=1.2.3 fuse

# Mount data disk and set to mount on startup
sudo fdisk /dev/sdc <<EOF
n
p
1
2048
67108863
p
w
EOF
sudo mkfs -t ext4 /dev/sdc1
sudo mkdir /mysql
sudo mount /dev/sdc1 /mysql
DISKUUID=$(sudo -i blkid | awk '{ print $2 }' | awk '{ print $2 }' FS='=' | tail -n 1 | tr -d '"')
sudo echo "UUID=$DISKUUID   /mysql   ext4   defaults,nofail   1   2" >> /etc/fstab

# Run cromwellazure on startup
sudo systemctl enable cromwellazure
