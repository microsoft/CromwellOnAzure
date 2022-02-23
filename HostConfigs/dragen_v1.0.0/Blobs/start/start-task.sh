#!/usr/bin/env bash

cd $docker_host_configuration

# This script should be executed on VM host in the directly as the rpm packages
# the host will be mounted at /host, the debs will be copied to /mnt
# then the container will nsenter and install everything against the host.

sudo yum install https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm -y
sudo yum install kernel-headers-$(uname -r) kernel-devel-$(uname -r) dkms -y

sudo yum install xrt*.rpm -y
sudo yum install xilinx*.rpm -y

sudo /opt/xilinx/xrt/bin/xbutil host_mem -d 0 --enable --size 1G
