#!/bin/bash
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

LAB_NAME=$1
STORAGE_ACCOUNT_NAME=$2
COSMOS_DB_NAME=$3
BATCH_ACCOUNT_NAME=$4
APP_INSIGHTS_ACCOUNT_NAME=$5

#Create directories
sudo mkdir /mnt/blobfusetmp-1
sudo mkdir /cromwell-app-config
sudo mkdir /cromwell-executions-1
sudo mkdir /cromwell-workflow-logs-1
sudo mkdir /inputs-1
sudo mkdir /cromwellazure

#Download Cromwell app config
sudo curl -o /cromwell-app-config/cromwell-application.conf https://raw.githubusercontent.com/microsoft/CromwellOnAzure/master/src/deploy-cromwell-on-azure/scripts/cromwell-application.conf?token=AFSS5T47CZWURCQTJLOEO6S5T54EU

#Install Docker Compose
sudo apt update
sudo apt install apt-transport-https ca-certificates curl software-properties-common
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu bionic stable"
sudo apt update
sudo apt-cache policy docker-ce
sudo apt -y install docker-ce
sudo curl -L https://github.com/docker/compose/releases/download/1.21.2/docker-compose-`uname -s`-`uname -m` -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
sudo docker-compose --version

#Download Docker Compose files
sudo curl -o /cromwellazure/docker-compose.yml https://raw.githubusercontent.com/microsoft/CromwellOnAzure/master/src/deploy-cromwell-on-azure/scripts/docker-compose.yml?token=AFSS5T6VNYRB46VKKUEAHCS5UYBPQ
sudo curl -o /cromwellazure/docker-compose.lab1.yml https://raw.githubusercontent.com/microsoft/CromwellOnAzure/master/src/deploy-cromwell-on-azure/scripts/docker-compose.lab1.yml?token=AFSS5T5QQUZKLG2NDOR3IOC5T54F6

#Replace variables in Docker Compose files
sudo sed -i "s/LABNAME/$LAB_NAME/g" /cromwellazure/docker-compose.yml
sudo sed -i "s/KEYVAULTNAME/$KEY_VAULT_NAME/g" /cromwellazure/docker-compose.yml
sudo sed -i "s/COSMOSDBNAME/$COSMOS_DB_NAME/g" /cromwellazure/docker-compose.yml
sudo sed -i "s/VARBATCHACCOUNTNAME/$BATCH_ACCOUNT_NAME/g" /cromwellazure/docker-compose.yml
sudo sed -i "s/VARAPPLICATIONINSIGHTSACCOUNTNAME/$APP_INSIGHTS_ACCOUNT_NAME/g" /cromwellazure/docker-compose.yml
sudo sed -i "s/STORAGEACCOUNTNAME/$STORAGE_ACCOUNT_NAME/g" /cromwellazure/docker-compose.yml
sudo sed -i "s/LABNAME/$LAB_NAME/g" /cromwellazure/docker-compose.lab1.yml
sudo sed -i "s/STORAGEACCOUNTNAME/$STORAGE_ACCOUNT_NAME/g" /cromwellazure/docker-compose.lab1.yml

#Install Blobfuse
sudo wget https://packages.microsoft.com/config/ubuntu/16.04/packages-microsoft-prod.deb
sudo dpkg -i packages-microsoft-prod.deb
sudo apt-get update
sudo apt-get -y install blobfuse fuse

#Download blobfuse mount script
sudo curl -o /usr/sbin/mount.blobfuse https://raw.githubusercontent.com/microsoft/CromwellOnAzure/master/src/deploy-cromwell-on-azure/scripts/mount.blobfuse?token=AFSS5T52ATB2YCIY3TODGIC5T54HY
sudo chmod +x /usr/sbin/mount.blobfuse

#Update fstab so blobfuse runs on startup
sudo echo "/usr/sbin/mount.blobfuse /cromwell-executions-1/ fuse _netdev,account_name=$STORAGE_ACCOUNT_NAME,container_name=cromwell-executions" >> /etc/fstab
sudo echo "/usr/sbin/mount.blobfuse /cromwell-workflow-logs-1/ fuse _netdev,account_name=$STORAGE_ACCOUNT_NAME,container_name=cromwell-workflow-logs" >> /etc/fstab
sudo echo "/usr/sbin/mount.blobfuse /inputs-1/ fuse _netdev,account_name=$STORAGE_ACCOUNT_NAME,container_name=inputs" >> /etc/fstab

# mount data disk and set to mount on startup
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
sudo mkdir /mysql-1
sudo mount /dev/sdc1 /mysql-1
DISKUUID=$(sudo -i blkid | awk '{ print $2 }' | awk '{ print $2 }' FS='=' | tail -n 1 | tr -d '"')
sudo echo "UUID=$DISKUUID   /mysql-1   ext4   defaults,nofail   1   2" >> /etc/fstab

#Execute fstab
sudo mount -a

#Install Azure CLI
sudo curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

#configure service to run at startup
sudo curl -o /cromwellazure/startup.sh https://raw.githubusercontent.com/microsoft/CromwellOnAzure/master/src/deploy-cromwell-on-azure/scripts/startup.sh?token=AFSS5T75HDT2VKY3IRPW7PK5UYBRI
sudo sed -i "s/STORAGEACCOUNTNAME/$STORAGE_ACCOUNT_NAME/g" /cromwellazure/startup.sh
sudo sed -i "s/LABNAME/$LAB_NAME/g" /cromwellazure/startup.sh
sudo chmod +x /cromwellazure/startup.sh
sudo curl -o /lib/systemd/system/cromwellazure.service https://raw.githubusercontent.com/microsoft/CromwellOnAzure/master/src/deploy-cromwell-on-azure/scripts/cromwellazure.service?token=AFSS5T4GE3LEANVIFNN6X225T54FM
sudo systemctl enable cromwellazure
