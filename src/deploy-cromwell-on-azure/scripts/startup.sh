#!/bin/bash
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

set -o errexit
set -o nounset

readonly log_file="/cromwellazure/startup.log"
touch $log_file
exec 1>$log_file
exec 2>&1

function write_log() {
    # Prepend the parameter value with the current datetime, if passed
    echo ${1+$(date --iso-8601=seconds) $1}
}

write_log "CromwellOnAzure startup log"
write_log

write_log "mount_containers.sh:"
cd /cromwellazure
./mount_containers.sh -a STORAGEACCOUNTNAME
write_log

write_log "Mounted blobfuse containers:"
findmnt -t fuse
write_log

write_log "docker-compose pull:"
docker-compose pull --ignore-pull-failures
write_log

write_log "docker-compose up:"
mkdir -p /cromwellazure/cromwell-tmp
docker-compose -f /cromwellazure/docker-compose.yml -f /cromwellazure/docker-compose.override.yml up -d
write_log

write_log "Startup complete"

# Keep the process running and call mount periodically to remount volumes that were dropped due to blobfuse crash
while true; do  
    sleep 30  
    mount -a -t fuse || write_log "mount error code: $?" 
done 
