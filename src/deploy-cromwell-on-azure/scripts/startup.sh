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

write_log "Stopping Docker containers to mount Azure Storage containers via blobfuse..."
docker-compose down

write_log "Generating Docker Compose .env file"
declare -A kv
while IFS='=' read key value; do kv[$key]=$value; done < <(awk 'NF > 0' env-*)
kv["CromwellImageSha"]=""
{ReplaceMySqlSha}
kv["TesImageSha"]=""
kv["TriggerServiceImageSha"]=""
rm -f .env && for key in "${!kv[@]}"; do echo "$key=${kv[$key]}" >> .env; done
write_log

storage_account_name=${kv["DefaultStorageAccountName"]}
managed_identity_client_id=${kv["ManagedIdentityClientId"]}

write_log "Checking account access (this could take awhile due to role assignment propagation delay)..."

while :
do
    write_log "Logging in with management identity..."
    if az login --identity --username $managed_identity_client_id > /dev/null; then
        write_log "Getting list of accessible subscriptions..."
        if subscription_ids=$(az account list --query [].id -o tsv); then
            write_log "Getting list of accessible resources..."
            # Initializing array with single empty string to avoid unbound variable error in bash version < 4.4
            resource_ids=("")

            for subscription_id in $subscription_ids
            do
                if resource_ids_in_single_sub=$(az resource list --subscription $subscription_id --query [].id -o tsv); then
                    resource_ids=("${resource_ids[@]}" "${resource_ids_in_single_sub[@]}")
                fi
            done

            write_log "Verifying that all required accounts (4) are present in the list of accessible resources..."

            if grep -q "${kv["DefaultStorageAccountName"]}" <<< "${resource_ids[@]}" \
                && grep -q "${kv["CosmosDbAccountName"]}" <<< "${resource_ids[@]}" \
                && grep -q "${kv["BatchAccountName"]}" <<< "${resource_ids[@]}" \
                && grep -q "${kv["ApplicationInsightsAccountName"]}" <<< "${resource_ids[@]}"; then

                write_log "Getting storage key..."
                if storage_key=$(az storage account keys list --account-name $storage_account_name --query "[?keyName=='key1'].value" -o tsv); then
                    write_log "Checking access to containers-to-mount file..."
                    if az storage blob exists --account-name $storage_account_name --account-key $storage_key --container-name configuration --name containers-to-mount > /dev/null; then
                        break
                    else
                        write_log "containers-to-mount file is not accessible (exit code $?), retrying..."
                    fi
                fi
            else
                write_log "Not all accounts are accessible, retrying..."
            fi
        else
            write_log "Subscriptions query failed with exit code $?, retrying..."
        fi
    else
        write_log "Management login request failed with exit code $?, retrying..."
    fi

    sleep 10
done

write_log "Account access OK"
write_log

write_log "Running docker-compose pull"
docker-compose pull --ignore-pull-failures || true
write_log

write_log "Getting image digests"
kv["CromwellImageSha"]=$(docker inspect --format='{{range (.RepoDigests)}}{{.}}{{end}}' ${kv["CromwellImageName"]})
{ReplaceMySqlShaSet}
kv["TesImageSha"]=$(docker inspect --format='{{range (.RepoDigests)}}{{.}}{{end}}' ${kv["TesImageName"]})
kv["TriggerServiceImageSha"]=$(docker inspect --format='{{range (.RepoDigests)}}{{.}}{{end}}' ${kv["TriggerServiceImageName"]})
rm -f .env && for key in "${!kv[@]}"; do echo "$key=${kv[$key]}" >> .env; done

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