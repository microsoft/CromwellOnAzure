#!/usr/bin/env bash

# This script should be executed on VM host in the directly as the rpm packages
# the host will be mounted at /host, the debs will be copied to /mnt
# then the container will nsenter and install everything against the host.

sudo yum install https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm -y
sudo yum install kernel-headers-$(uname -r) kernel-devel-$(uname -r) dkms -y

curl -L https://www.xilinx.com/bin/public/openDownload?filename=xrt_202020.2.8.832_7.4.1708-x86_64-xrt.rpm -o xrt_202020.2.8.832_7.4.1708-x86_64-xrt.rpm
curl -L https://www.xilinx.com/bin/public/openDownload?filename=xrt_202020.2.8.832_7.4.1708-x86_64-azure.rpm -o xrt_202020.2.8.832_7.4.1708-x86_64-azure.rpm
curl -L https://www.xilinx.com/bin/public/openDownload?filename=xilinx-u250-gen3x16-xdma-platform-2.1-3.noarch.rpm.tar.gz  -o xilinx-u250-gen3x16-xdma-platform-2.1-3.noarch.rpm.tar.gz
curl -L https://www.xilinx.com/bin/public/openDownload?filename=xilinx-u250-gen3x16-xdma-validate-2.1-3005608.1.noarch.rpm -o xilinx-u250-gen3x16-xdma-validate-2.1-3005608.1.noarch.rpm

sudo yum install xrt*.rpm -y
sudo yum install xilinx*.rpm -y

sudo /opt/xilinx/xrt/bin/xbutil host_mem -d 0 --enable --size 1G