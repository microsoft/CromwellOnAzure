#!/bin/bash
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

echo "Start Task: Running, Checking if Docker is installed"
if ! docker -v ; then
  echo "Start Task: Installing Docker"
  sudo apt update && \
  sudo apt install -y apt-transport-https ca-certificates curl software-properties-common && \
  sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
  sudo apt-key add - && \
  sudo add-apt-repository -y "deb[arch = amd64] https://download.docker.com/linux/ubuntu focal stable" && \
  sudo apt update && \
  sudo apt install -y docker-ce
fi
echo "Start Task: Completed, Docker is currently installed"
