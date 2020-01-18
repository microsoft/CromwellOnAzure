#!/bin/bash
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

#!/bin/bash

while getopts a: option
do
  case "${option}" in
    a) default_storage_account=${OPTARG};
  esac
done

get_list_of_containers_to_mount () {
  local -n result=$1
  storage_token=$(curl -s -H Metadata:true "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https://$default_storage_account.blob.core.windows.net" | grep -o '"access_token":"[^"]*' | grep -o '[^"]*$')
  containers=$(curl -s -X GET "https://$default_storage_account.blob.core.windows.net/configuration/containers-to-mount" -H "Authorization: Bearer $storage_token" -H "x-ms-version: 2018-03-28" -d '' )
  containers=$(tr -d "[:blank:]" <<< "$containers")    # remove all spaces
  containers=$(grep "^[^#]" <<< "$containers")    # remove all comment lines
  result=($containers)
}

get_accessible_storage_containers () {
  local -n result=$1

  mgmt_token=$(curl -s -H Metadata:true "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https://management.azure.com" | grep -o '"access_token":"[^"]*' | grep -o '[^"]*$')
  storage_token=$(curl -s -H Metadata:true "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https://storage.azure.com" | grep -o '"access_token":"[^"]*' | grep -o '[^"]*$')

  subscription_ids=$(curl -s -X GET "https://management.azure.com/subscriptions/?api-version=2019-08-01" -H "Authorization: Bearer $mgmt_token" | grep -Po '"id":"/subscriptions/\K([^"]*)' )

  for subscription_id in $subscription_ids; do
    resource_filter="resourceType%20eq%20'Microsoft.Storage/storageAccounts'"

    account_ids=$(curl -s -X GET "https://management.azure.com/subscriptions/$subscription_id/resources?%24filter=$resource_filter&api-version=2019-05-10" -H "Authorization: Bearer $mgmt_token" \
      | grep -Po "/subscriptions/[^\"]*/providers/Microsoft.Storage/storageAccounts/[^\"]*")

    for account_id in $account_ids; do
      account_name=$(grep -Po '/subscriptions/[^"]*/providers/Microsoft.Storage/storageAccounts/\K([^"]*)' <<< "$account_id")
      account_key=$(curl -s -X POST "https://management.azure.com/$account_id/listKeys?api-version=2016-12-01" -H "Authorization: Bearer $mgmt_token" -d '' | grep -Po '"key1","value":"\K([^"]*)' )
      container_names=$(curl -s -X GET "https://$account_name.blob.core.windows.net/?comp=list" -H "Authorization: Bearer $storage_token" -H "x-ms-version: 2018-03-28" -d '' | grep -Po '<Name>\K([^<]*)' )

      for container_name in $container_names; do
        result["$account_name/$container_name"]=$account_key
      done
    done
  done
}

echo $default_storage_account
declare -a container_patterns_to_mount
declare -A accessible_containers
declare -A include_patterns

get_list_of_containers_to_mount container_patterns_to_mount

echo "Containers to mount:"
for x in "${container_patterns_to_mount[@]}"; do echo "$x}"; done

container_patterns_to_mount=("${container_patterns_to_mount[@]/.blob.core.windows.net/}")
container_patterns_to_mount=("${container_patterns_to_mount[@]/https:\/\//}")
container_patterns_to_mount=("${container_patterns_to_mount[@]/http:\/\//}")

for pattern in "${container_patterns_to_mount[@]}"; do
  acct_and_cont=$(expr "$pattern" : '^[-/]*\([^?]*\)')    # remove leading "-" and "/", and the SAS token
  acct_and_cont=${acct_and_cont/%\//}    # remove trailing "/"
  sas=$(expr "$pattern" : '[^?]*\(.*\)')
  [[ $pattern != -* ]] && include_patterns[$acct_and_cont]=$sas || exclude_patterns+=($acct_and_cont)
done

echo ""
echo "account/container patterns to include:"
for x in "${!include_patterns[@]}"; do echo "$x -> ${include_patterns[$x]}"; done
echo ""
echo "account/container patterns to exclude:"
printf '%s\n' "${exclude_patterns[@]}"

get_accessible_storage_containers accessible_containers

echo ""
echo "Containers accessible to the VM:"
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
  for exclude_pattern in "${exclude_patterns[@]}"; do [[ $x == $exclude_pattern ]] && include="false"; done

  [[ $include == "false" ]] && continue
  [[ $(ps aux | grep -c $x) -eq 2 ]] && continue

  account_name=$(expr "$x" : '\(.*\)/.*') \
    && container_name=$(expr "$x" : '.*/\(.*\)') \
    && mount_path="/mnt/$account_name/$container_name" \
    && { [[ $account_name == $default_storage_account && ( $container_name == "configuration" || $container_name == "cromwell-executions" || $container_name == "cromwell-workflow-logs" ) ]] && mount_path_in_docker_container="/$container_name" || mount_path_in_docker_container="/$account_name/$container_name" ; } \
    && mkdir -p $mount_path || (umount -l $mount_path && mkdir -p $mount_path) \
    && sudo echo "/usr/sbin/mount.blobfuse $mount_path fuse _netdev,account_name=$account_name,container_name=$container_name,account_key=$account_key" >> /etc/fstab \
    && echo "Added entry in fstab for container $container_name from account $account_name with mount path $mount_path on host and $mount_path_in_docker_container in docker using the account key." \
    && cromwell_volumes+="      - type: bind\n        source: $mount_path\n        target: $mount_path_in_docker_container\n" ;
done

# Mount containers that were specified with URL+SAS syntax, filtered by the exclude list
for x in "${!include_patterns[@]}"; do
  sas_token=${include_patterns[$x]}
  [[ $sas_token == "" ]] && continue
  include="true"
  for exclude_pattern in "${exclude_patterns[@]}"; do [[ $x == $exclude_pattern ]] && include="false"; done

  [[ $include == "false" ]] && continue
  [[ $(ps aux | grep -c $x) -eq 2 ]] && continue

  account_name=$(expr "$x" : '\(.*\)/.*') \
    && container_name=$(expr "$x" : '.*/\(.*\)') \
    && mount_path="/mnt/$account_name/$container_name" \
    && mount_path_in_docker_container="/$account_name/$container_name" \
    && mkdir -p $mount_path || (umount -l $mount_path && mkdir -p $mount_path) \
    && sudo echo "/usr/sbin/mount.blobfuse $mount_path fuse _netdev,account_name=$account_name,container_name=$container_name,sas_token=$sas_token" >> /etc/fstab \
    && echo "Added entry in fstab for container $container_name from account $account_name with mount path $mount_path on host and $mount_path_in_docker_container in docker using the SAS token." \
    && tes_ext_storage_containers+="https://$account_name.blob.core.windows.net/$container_name$sas_token;" \
    && cromwell_volumes+="      - type: bind\n        source: $mount_path\n        target: $mount_path_in_docker_container\n" ;
done

docker_compose_overrides="version: \"3.6\"\nservices:\n  cromwell:\n    volumes:\n$cromwell_volumes  tes:\n    environment:\n      - ExternalStorageContainers=$tes_ext_storage_containers\n"
echo -e "$docker_compose_overrides" > "docker-compose.override.yml"

# Execute fstab
sudo mount -av -t fuse