#!/bin/bash
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
echo "CromwellOnAzure startup task 1"
cd /cromwellazure/
docker-compose pull
echo "CromwellOnAzure startup task 2"
docker-compose -f /cromwellazure/docker-compose.yml -f /cromwellazure/docker-compose.lab1.yml -p lab1 up -d
echo "CromwellOnAzure done with startup."