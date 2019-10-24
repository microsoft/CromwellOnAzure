#!/bin/bash
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

set -o errexit
set -o nounset

readonly log_file="/cromwellazure/startup.log"
touch $log_file
exec 1>$log_file
exec 2>&1

echo "CromwellOnAzure startup log"
echo
echo "mount_containers.sh:"
cd /cromwellazure
./mount_containers.sh -a STORAGEACCOUNTNAME
echo
echo "Mounted blobfuse containers:"
findmnt -t fuse
echo
echo "docker-compose pull:"
docker-compose pull --ignore-pull-failures
echo
echo "docker-compose up:"
docker-compose -f /cromwellazure/docker-compose.yml -f /cromwellazure/docker-compose.override.yml up -d
echo
echo "Startup complete"

# keep the process alive so blobfuse mounts stay mounted
while true; do sleep 10000; done