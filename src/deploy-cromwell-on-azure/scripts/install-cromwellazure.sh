#!/bin/bash
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

set -o errexit
set -o nounset
set -o errtrace

trap 'write_log "Install failed with exit code $?"' ERR

readonly log_file="/data/cromwellazure/install.log"
touch $log_file
exec 1>>$log_file
exec 2>&1

function write_log() {
    # Prepend the parameter value with the current datetime, if passed
    echo ${1+$(date --iso-8601=seconds) $1}
}

function wait_for_apt_locks() {
    i=0

    while fuser /var/lib/dpkg/lock /var/lib/dpkg/lock-frontend /var/lib/apt/lists/lock /var/cache/apt/archives/lock >/dev/null 2>&1; do
        if [ $i -gt 20 ]; then
            write_log 'Timed out while waiting for release of apt locks'
            exit 1
        else
            write_log 'Waiting for release of apt locks'
            sleep 30
        fi

        let i=i+1
    done
}

write_log "Verifying that no other package updates are in progress"
wait_for_apt_locks

write_log "Install starting"

write_log "Installing docker and docker-compose"
sudo apt update
sudo apt install -y apt-transport-https ca-certificates curl software-properties-common
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo add-apt-repository -y "deb [arch=amd64] https://download.docker.com/linux/ubuntu bionic stable"
sudo apt update
sudo apt-cache policy docker-ce
sudo apt install -y docker-ce
sudo curl -L "https://github.com/docker/compose/releases/download/1.26.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
sudo docker-compose --version

write_log "Installing blobfuse"
ubuntuVersion=$(lsb_release -ar 2>/dev/null | grep -i release | cut -s -f2)
sudo wget https://packages.microsoft.com/config/ubuntu/$ubuntuVersion/packages-microsoft-prod.deb
sudo dpkg -i packages-microsoft-prod.deb
sudo apt update
sudo apt install -y --allow-downgrades blobfuse=1.4.3 fuse

write_log "Installing az cli"
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

write_log "Applying security patches"
sudo unattended-upgrades -v

if [ -d "/mysql" ]; then
    write_log "Previous /mysql exists, moving it to /data/mysql"
    cd /data/cromwellazure
    sudo docker-compose stop &>/dev/null
    sudo mv /mysql /data
fi

sudo mkdir -p /data/mysql

if [ -d "/cromwellazure" ] && [ ! -L "/cromwellazure" ]; then
    write_log "Previous /cromwellazure exists, using it to create /data/cromwellazure/env-01-account-names.txt and env-04-settings.txt"
    egrep -o 'DefaultStorageAccountName.*|CosmosDbAccountName.*|BatchAccountName.*|ApplicationInsightsAccountName.*' /cromwellazure/docker-compose.yml | sort -u > /data/cromwellazure/env-01-account-names.txt
    egrep -o 'DisableBatchScheduling.*|AzureOfferDurableId.*' /cromwellazure/docker-compose.yml | sort -u > /data/cromwellazure/env-04-settings.txt
    echo "UsePreemptibleVmsOnly=false" >> /data/cromwellazure/env-04-settings.txt
    grep -q 'DisableBatchScheduling' /data/cromwellazure/env-04-settings.txt || echo "DisableBatchScheduling=false" >> /data/cromwellazure/env-04-settings.txt
    write_log "Moving previous /cromwellazure to /cromwellazure-backup"
    sudo mv /cromwellazure /cromwellazure-backup
fi

if [ ! -L "/cromwellazure" ]; then
    write_log "Creating symlink /cromwellazure -> /data/cromwellazure"
    sudo ln -s /data/cromwellazure /cromwellazure
fi

write_log "Disabling the Docker service, because the cromwellazure service is responsible for starting Docker"
sudo systemctl disable docker

write_log "Enabling cromwellazure service"
sudo systemctl enable cromwellazure.service

write_log "Install complete"
