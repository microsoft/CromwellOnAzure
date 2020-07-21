#!/bin/bash
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

set -o errexit
set -o nounset
set -o errtrace

trap 'write_log "Startup failed with exit code $?"' ERR

cd /data/cromwellazure

readonly log_file="startup.log"
touch $log_file
exec 1>$log_file
exec 2>&1

function write_log() {
    # Prepend the parameter value with the current datetime, if passed
    echo ${1+$(date --iso-8601=seconds) $1}
}

write_log "CromwellOnAzure startup log"
write_log

write_log "Generating Docker Compose .env file"
declare -A kv
while IFS='=' read key value; do kv[$key]=$value; done < <(awk 'NF > 0' env-*)
kv["CromwellImageSha"]=""
kv["MySqlImageSha"]=""
kv["TesImageSha"]=""
kv["TriggerServiceImageSha"]=""
rm -f .env && for key in "${!kv[@]}"; do echo "$key=${kv[$key]}" >> .env; done
write_log

write_log "Running docker-compose pull"
docker-compose pull --ignore-pull-failures || true
write_log

write_log "Getting image digests"
kv["CromwellImageSha"]=$(docker inspect --format='{{range (.RepoDigests)}}{{.}}{{end}}' ${kv["CromwellImageName"]})
kv["MySqlImageSha"]=$(docker inspect --format='{{range (.RepoDigests)}}{{.}}{{end}}' ${kv["MySqlImageName"]})
kv["TesImageSha"]=$(docker inspect --format='{{range (.RepoDigests)}}{{.}}{{end}}' ${kv["TesImageName"]})
kv["TriggerServiceImageSha"]=$(docker inspect --format='{{range (.RepoDigests)}}{{.}}{{end}}' ${kv["TriggerServiceImageName"]})
rm -f .env && for key in "${!kv[@]}"; do echo "$key=${kv[$key]}" >> .env; done

storage_account_name=${kv["DefaultStorageAccountName"]}

write_log "Mounting containers (default storage account = $storage_account_name)"
./mount_containers.sh -a $storage_account_name
write_log

write_log "Mounted containers:"
findmnt -t fuse
write_log

write_log "Running docker-compose up"
mkdir -p /mnt/cromwell-tmp
docker-compose -f docker-compose.yml -f docker-compose.override.yml up -d
write_log

write_log "Startup complete"

# Keep the process running and call mount periodically to remount volumes that were dropped due to blobfuse crash
while true; do
    sleep 30
    mount -a -t fuse || write_log "mount error code: $?"
done