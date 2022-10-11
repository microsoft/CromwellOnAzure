#!/usr/bin/env bash

sudo yum -y update
sudo yum -y install kernel-devel kernel-headers
#sudo reboot

sudo yum -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm

curl -L https://www.xilinx.com/bin/public/openDownload?filename=xrt_202020.2.8.832_7.4.1708-x86_64-xrt.rpm -o xrt_202020.2.8.832_7.4.1708-x86_64-xrt.rpm
curl -L https://www.xilinx.com/bin/public/openDownload?filename=xrt_202020.2.8.832_7.4.1708-x86_64-azure.rpm -o xrt_202020.2.8.832_7.4.1708-x86_64-azure.rpm

sudo yum -y install xrt*.rpm

curl -L https://www.xilinx.com/bin/public/openDownload?filename=xilinx-u250-gen3x16-xdma-platform-2.1-3.noarch.rpm.tar.gz  -o xilinx-u250-gen3x16-xdma-platform-2.1-3.noarch.rpm.tar.gz
curl -L https://www.xilinx.com/bin/public/openDownload?filename=xilinx-u250-gen3x16-xdma-validate-2.1-3005608.1.noarch.rpm -o xilinx-u250-gen3x16-xdma-validate-2.1-3005608.1.noarch.rpm
tar -zxvf xilinx-u250-gen3x16-xdma-platform-2.1-3.noarch.rpm.tar.gz

sudo yum -y install xilinx*.rpm

sudo systemctl enable mpd
sudo systemctl start mpd

sudo /opt/xilinx/xrt/bin/xbutil host_mem -d 0 --enable --size 1G
