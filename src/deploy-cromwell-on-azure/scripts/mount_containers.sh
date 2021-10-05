#!/bin/bash
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

set -o errexit
set -o nounset
set -o errtrace

trap 'echo_with_ts "mount_containers failed with exit code $?"' ERR

while getopts a: option
do
  case "${option}" in
    a) default_storage_account=${OPTARG};;
  esac
done

function echo_with_ts() {
    # Prepend the parameter value with the current datetime, if passed
    echo ${1+$(date --iso-8601=seconds) $1}
}

get_list_of_containers_to_mount () {
  local -n result=$1
  echo_with_ts "Getting access key of the $default_storage_account storage account"
  key=$(az storage account keys list --account-name $default_storage_account --query "[?keyName=='key1'].value" -o tsv)
  echo_with_ts "Getting list of containers to mount from containers-to-mount file"
  az storage blob download --account-name $default_storage_account --auth-mode key --account-key=$key --container-name configuration --name containers-to-mount --file /tmp/containers-to-mount > /dev/null 2>&1
  containers=$(</tmp/containers-to-mount)
  containers=$(tr -d "[:blank:]" <<< "$containers")    # remove all spaces
  containers=$(grep "^[^#]" <<< "$containers")    # remove all comment lines
  result=($containers)
}

get_accessible_storage_containers () {
  local -n result=$1

  echo_with_ts "Getting list of accessible subscriptions"
  subscription_ids=$(az account list --query [].id -o tsv)

  for subscription_id in $subscription_ids; do
    echo_with_ts "Getting list of storage accounts accessible by this VM in subscription $subscription_id"
    account_names=$(az storage account list --subscription $subscription_id --query [].name -o tsv)

    for account_name in $account_names; do
      echo_with_ts "Getting access key for storage account $account_name"
      account_key=$(az storage account keys list --subscription $subscription_id --account-name $account_name --query "[?keyName=='key1'].value" -o tsv)
      echo_with_ts "Getting list of containers for storage account $account_name"
      container_names=$(az storage container list --subscription $subscription_id --account-name $account_name --account-key=$account_key --num-results '*' --query [].name -o tsv)

      for container_name in $container_names; do
        result["$account_name/$container_name"]=$account_key
      done
    done
  done
}

declare -a container_patterns_to_mount
declare -A accessible_containers
declare -A include_patterns
declare -a exclude_patterns

echo_with_ts "mount_containers starting"

get_list_of_containers_to_mount container_patterns_to_mount

echo_with_ts "Containers to mount:"
printf "%s\n" "${container_patterns_to_mount[@]-}" 

container_patterns_to_mount=("${container_patterns_to_mount[@]/.blob.core.windows.net/}")
container_patterns_to_mount=("${container_patterns_to_mount[@]/https:\/\//}")
container_patterns_to_mount=("${container_patterns_to_mount[@]/http:\/\//}")

for pattern in "${container_patterns_to_mount[@]}"; do
  acct_and_cont=$(expr "$pattern" : '^[-/]*\([^?]*\)')    # remove leading "-" and "/", and the SAS token
  acct_and_cont=${acct_and_cont/%\//}    # remove trailing "/"
  sas=$(expr "$pattern" : '[^?]*\(.*\)') || sas=""
  [[ $pattern != -* ]] && include_patterns[$acct_and_cont]=$sas || exclude_patterns+=($acct_and_cont)
done

echo ""
echo_with_ts "account/container patterns to include:"
for x in "${!include_patterns[@]}"; do echo "$x -> ${include_patterns[$x]}"; done
echo ""
echo_with_ts "account/container patterns to exclude:"
printf "%s\n" "${exclude_patterns[@]-}" 

get_accessible_storage_containers accessible_containers

echo ""
echo_with_ts "Containers accessible to the VM:"
for x in "${!accessible_containers[@]}"; do echo "$x"; done
echo ""

cromwell_volumes=''
tes_ext_storage_containers=''

# Remove existing fstab entries, we'll re-add the active ones below
sudo sed -i "/mount.blobfuse/d" /etc/fstab

# Mount containers that are accessible by the VM MSI identity, filtered by the include and exclude lists
for x in "${!accessible_containers[@]}"; do
  account_key=${accessible_containers[$x]}
  include="false"
  for include_pattern in "${!include_patterns[@]}"; do [[ $x == $include_pattern ]] && include="true"; done
  for exclude_pattern in "${exclude_patterns[@]-}"; do [[ $x == $exclude_pattern ]] && include="false"; done

  [[ $include == "true" ]] \
    && account_name=$(expr "$x" : '\(.*\)/.*') \
    && container_name=$(expr "$x" : '.*/\(.*\)') \
    && mount_path="/mnt/$account_name/$container_name" \
    && { [[ $account_name == $default_storage_account && ( $container_name == "configuration" || $container_name == "cromwell-executions" || $container_name == "cromwell-workflow-logs" ) ]] && mount_path_in_docker_container="/$container_name" || mount_path_in_docker_container="/$account_name/$container_name" ; } \
    && mkdir -p $mount_path \
    && sudo echo "/usr/sbin/mount.blobfuse $mount_path fuse _netdev,account_name=$account_name,container_name=$container_name,account_key=$account_key" >> /etc/fstab \
    && echo_with_ts "Added entry in fstab for container $container_name from account $account_name with mount path $mount_path on host and $mount_path_in_docker_container in docker using the account key." \
    && cromwell_volumes+="      - type: bind\n        source: $mount_path\n        target: $mount_path_in_docker_container\n" ;
done

# Mount containers that were specified with URL+SAS syntax, filtered by the exclude list
for x in "${!include_patterns[@]}"; do
  sas_token=${include_patterns[$x]}
  [[ $sas_token == "" ]] && continue
  include="true"
  for exclude_pattern in "${exclude_patterns[@]-}"; do [[ $x == $exclude_pattern ]] && include="false"; done

  [[ $include == "true" ]] \
    && account_name=$(expr "$x" : '\(.*\)/.*') \
    && container_name=$(expr "$x" : '.*/\(.*\)') \
    && mount_path="/mnt/$account_name/$container_name" \
    && mount_path_in_docker_container="/$account_name/$container_name" \
    && mkdir -p $mount_path \
    && sudo echo "/usr/sbin/mount.blobfuse $mount_path fuse _netdev,account_name=$account_name,container_name=$container_name,sas_token=$sas_token" >> /etc/fstab \
    && echo_with_ts "Added entry in fstab for container $container_name from account $account_name with mount path $mount_path on host and $mount_path_in_docker_container in docker using the SAS token." \
    && tes_ext_storage_containers+="https://$account_name.blob.core.windows.net/$container_name$sas_token;" \
    && cromwell_volumes+="      - type: bind\n        source: $mount_path\n        target: $mount_path_in_docker_container\n" ;
done

docker_compose_overrides="version: \"3.6\"\nservices:\n  cromwell:\n    volumes:\n$cromwell_volumes  tes:\n    environment:\n      - ExternalStorageContainers=$tes_ext_storage_containers\n"
echo -e "$docker_compose_overrides" > "docker-compose.override.yml"

# Unmount existing mounts and remount using the updated /etc/fstab
sudo umount -a -f -t fuse
sudo rm -f /data/cromwellazure/mount.blobfuse.log
sudo mount -av -t fuse

echo_with_ts "mount_containers completed successfully"