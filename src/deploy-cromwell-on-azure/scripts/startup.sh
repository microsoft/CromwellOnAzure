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

write_log "Checking account access (this could take awhile due to role assignment propagation delay)..."

while :
do
    write_log "Getting management token..."
    IFS='~' read content http_status <<< $(curl -s -H Metadata:true --write-out '~%{http_code}' "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https://management.azure.com")

    if [ "$http_status" == "200" ]; then
        mgmt_token=$(grep -Po '"access_token":"\K([^"]*)' <<< $content)

        write_log "Getting list of accessible subscriptions..."
        IFS='~' read content http_status <<< $(curl -s -X GET --write-out '~%{http_code}' "https://management.azure.com/subscriptions?api-version=2019-05-10" -H "Authorization: Bearer $mgmt_token" -d '')

        if [ "$http_status" == "200" ]; then
            subscription_ids=( $(grep -Po '"id":"/subscriptions/\K([^"]*)' <<< $content) )

            write_log "Getting list of accessible resources..."
            resource_ids=()

            for subscription_id in "${subscription_ids[@]}"
            do
                IFS='~' read content http_status <<< $(curl -s -X GET --write-out '~%{http_code}' "https://management.azure.com/subscriptions/$subscription_id/resources?api-version=2019-05-10" -H "Authorization: Bearer $mgmt_token" -d '')

                if [ "$http_status" == "200" ]; then
                    resource_ids_in_single_sub=( $(grep -Po '"id":"\K([^"]*)' <<< $content) )
                    resource_ids=("${resource_ids[@]}" "${resource_ids_in_single_sub[@]}")
                fi
            done

            write_log "Verifying that all required accounts (4) are present in the list of accessible resources..."

            if grep -q "${kv["DefaultStorageAccountName"]}" <<< "${resource_ids[@]}" \
                && grep -q "${kv["CosmosDbAccountName"]}" <<< "${resource_ids[@]}" \
                && grep -q "${kv["BatchAccountName"]}" <<< "${resource_ids[@]}" \
                && grep -q "${kv["ApplicationInsightsAccountName"]}" <<< "${resource_ids[@]}"; then

                write_log "Getting storage token..."
                IFS='~' read content http_status <<< $(curl -s -H Metadata:true --write-out '~%{http_code}' "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https://$storage_account_name.blob.core.windows.net")

                if [ "$http_status" == "200" ]; then
                    storage_token=$(grep -Po '"access_token":"\K([^"]*)' <<< $content)

                    write_log "Checking access to containers-to-mount file..."
                    http_status=$(curl -o /dev/null -I -L -s -w "%{http_code}" -H "Authorization: Bearer $storage_token" -H "x-ms-version: 2018-03-28" "https://$storage_account_name.blob.core.windows.net/configuration/containers-to-mount")

                    if [ "$http_status" == "200" ]; then
                        break
                    else
                        write_log "containers-to-mount file is not accessible (status $http_status), retrying..."
                    fi
                fi
            else
                write_log "Not all accounts are accessible, retrying..."
            fi
        else
            write_log "Subscriptions query failed with $http_status, $content, retrying..."
        fi
    else
        write_log "Management token request failed with $http_status, $content, retrying..."
    fi

    sleep 10
done

write_log "Account access OK"
write_log

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