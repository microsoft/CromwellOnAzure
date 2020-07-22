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


write_log "Checking account access (this could take awhile due to role assignment propagation delay)..."

subscription_id=$(curl -s -H Metadata:true "http://169.254.169.254/metadata/instance/compute/subscriptionId?api-version=2017-08-01&format=text")

while :
do
    write_log "Getting management token..."

    response=$(curl -s -H Metadata:true --write-out '~%{http_code}' "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https://management.azure.com") \
        && IFS='~' read content http_status <<< $response \
        && [[ "$http_status" == "200" ]] \
        && mgmt_token=$(grep -o '"access_token":"[^"]*' <<< $content | grep -o '[^"]*$') \
        || write_log "Management token request failed with $http_status, $content"

    [ "$http_status" == "200" ] && break || sleep 10
done

while :
do
    while :
    do
        write_log "Getting list of accessible resources..."

        response=$(curl -s -X GET --write-out '~%{http_code}' "https://management.azure.com/subscriptions/$subscription_id/resources?api-version=2019-05-10" -H "Authorization: Bearer $mgmt_token" -d '') \
            && IFS='~' read content http_status <<< $response \
            && [[ "$http_status" == "200" ]] \
            && resource_ids=$(grep -Po '"id":"\K([^"]*)' <<< $content) \
            || write_log "Resource query failed with $http_status, $content"

            [ "$http_status" == "200" ] && break || sleep 10
    done

    write_log "Verifying that all required accounts (4) are present in the list of accessible resources..."

    if grep -q "${kv["DefaultStorageAccountName"]}" <<< "$resource_ids" \
            && grep -q "${kv["CosmosDbAccountName"]}" <<< "$resource_ids" \
            && grep -q "${kv["BatchAccountName"]}" <<< "$resource_ids" \
            && grep -q "${kv["ApplicationInsightsAccountName"]}" <<< "$resource_ids"; then
        break
    else
        write_log "Not all accounts are accessible, retrying..."
        sleep 10
    fi
done

write_log "Account access OK"
write_log

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