#!/bin/bash
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

set -o errexit
set -o nounset
set -o errtrace

exec &> >(logger --tag coa --skip-empty)

trap 'echo "mount-data-disk failed with exit code $?"' ERR

echo "mount-data-disk starting"

datadisk=/dev/disk/azure/scsi1/lun0
mountpoint=/data

if ! grep -q $datadisk /etc/fstab; then

    if ! [ -h "$datadisk" ] ; then
        echo "Error: Disk $datadisk does not exist"
        exit 1
    fi

    if ! $(lsblk -n $datadisk --output FSTYPE,TYPE | grep disk | grep -q ext4); then
        echo "Creating file system on $datadisk"
        sudo mkfs.ext4 -F $datadisk
    fi

    echo "Adding $mountpoint to fstab"
    sudo su -c "echo '$datadisk   $mountpoint   ext4   defaults,nofail   1   2' >> /etc/fstab"

    echo "Mounting $datadisk to $mountpoint"
    sudo mkdir -p $mountpoint
    sudo mount $mountpoint
fi

echo "mount-data-disk complete"
