#!/bin/bash

# Usage: deploy-private-coa.sh <azure_cloud_name> <subscription> <location>

azure_cloud_name=$1
subscription=$2
location=$3

# Ensure that all arguments are provided
if [ -z "$azure_cloud_name" ] || [ -z "$subscription" ] || [ -z "$location" ]; then
    echo "Usage: $0 <azure_cloud_name> <subscription> <location>"
    exit 1
fi

prefix="coa"
coa_identifier="${prefix}coa"
resource_group_name="${prefix}-coa-main"
aks_name="${prefix}coaaks"
aks_resource_group_name="${prefix}-coa-aks-nodes"
vnet_name="${prefix}-coa-vnet"
subnet_name="${prefix}-coa-subnet"
vmsubnet_name="${prefix}-coa-aks-subnet"
batchsubnet_name="${prefix}-coa-batch-subnet"
sqlsubnet_name="${prefix}-sql-subnet"
mycontainerregistry="${prefix}coacr"
storage_account_name="${prefix}coastorage"
managed_identity_name="${prefix}-coa-mi"
private_endpoint_name="${prefix}-coa-pe"
private_endpoint_name_storage="${prefix}-coa-pe-storage"
private_endpoint_name_cr="${prefix}-coa-pe-storage-cr"
private_cr_dns_zone_name="${prefix}-coa-cr-dns-zone"
private_cr_zone_name="privatelink.azurecr.io"
tes_image_name="mcr.microsoft.com/CromwellOnAzure/tes:5.3.0.10760"
trigger_service_image_name="mcr.microsoft.com/CromwellOnAzure/triggerservice:5.3.0.10760"
temp_deployer_vm_name="${prefix}-coa-deploy"
coa_binary="deploy-cromwell-on-azure"
coa_binary_path="/tmp/coa"

# Function to create a resource group if it doesn't exist
create_resource_group_if_not_exists() {
  local rg_name=$1
  local rg_location=$2
  # Check if the resource group exists
  if [ $(az group exists --name "$rg_name") = false ]; then
    echo "Creating resource group '$rg_name' in location '$rg_location'."
    az group create --name "$rg_name" --location "$rg_location"
  else
    echo "Resource group '$rg_name' already exists."
  fi
}

rm ../ga4gh-tes/nuget.config

if [ -f "./deploy-cromwell-on-azure-linux" ]; then
    # use pre-existing deployer binary for Linux in the same folder
    coa_binary="deploy-cromwell-on-azure-linux"
elif [ -f "./deploy-cromwell-on-azure" ]; then
    # use pre-existing generic deployer binary in the same folder
    coa_binary="deploy-cromwell-on-azure"
else
    # publish a new deployer binary
    dotnet publish -r linux-x64 -c Release -o ./ /p:PublishSingleFile=true /p:DebugType=none /p:IncludeNativeLibrariesForSelfExtract=true
fi

# Create the resource group if it doesn't exist
create_resource_group_if_not_exists $resource_group_name $location

echo "Creating identity..."
managed_identity_id=$(az identity create -g $resource_group_name -n $managed_identity_name -l $location --query id --output tsv)
# Assign the 'Owner' role to the managed identity for the resource group
echo "Waiting for identity to propagate..."
sleep 10 # Waits for 10 seconds

echo "Assigning owner to identity..."
az role assignment create --assignee-principal-type "ServicePrincipal" --assignee-object-id $(az identity show --name $managed_identity_name --resource-group $resource_group_name --query "principalId" -o tsv) --role "Owner" --scope /subscriptions/$subscription/resourceGroups/$resource_group_name

echo "Creating VM jumpbox within the virtual network to deploy from..."
vm_public_ip=$(az vm create \
    --resource-group $resource_group_name \
    --name $temp_deployer_vm_name \
    --image Ubuntu2204 \
    --admin-username azureuser \
    --generate-ssh-keys \
    --query publicIpAddress -o tsv)

#echo "Assigning identity to VM..."
#az vm identity assign --resource-group $resource_group_name --name $temp_deployer_vm_name --identities $managed_identity_id

echo "Opening port 22 for SSH access..."
az vm open-port --port 22 --resource-group $resource_group_name --name $temp_deployer_vm_name

echo "Waiting for port to open..."
sleep 10 # Waits for 10 seconds

echo "Installing AZ CLI and logging in..."
ssh -o StrictHostKeyChecking=no azureuser@$vm_public_ip "curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash; az cloud set -n $azure_cloud_name; az login --use-device-code"

ssh -o StrictHostKeyChecking=no azureuser@$vm_public_ip "mkdir -p /tmp/coa"

echo "Copying CoA deployment binary..."
scp -o StrictHostKeyChecking=no $coa_binary azureuser@$vm_public_ip:/tmp/coa/$coa_binary

echo "Installing Helm..."
ssh -o StrictHostKeyChecking=no azureuser@${vm_public_ip} "curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash"

echo "Setting CoA deployer binary to executable..."
ssh -o StrictHostKeyChecking=no azureuser@$vm_public_ip "chmod +x $coa_binary_path/$coa_binary"
echo "Executing CoA deployer binary..."
ssh -o StrictHostKeyChecking=no azureuser@$vm_public_ip "$coa_binary_path/$coa_binary \
    --IdentityResourceId $managed_identity_id \
    --SubscriptionId $subscription \
    --ResourceGroupName $resource_group_name \
    --RegionName $location \
    --AzureCloudName $azure_cloud_name \
    --MainIdentifierPrefix $coa_identifier \
    --PrivateNetworking true \
    --DisableBatchNodesPublicIpAddress true \
    --AksClusterName $aks_name \
    --AksNodeResourceGroupName $aks_resource_group_name \
    --StorageAccountName $storage_account_name \
    --VnetName $vnet_name \
    --VnetResourceGroupName $resource_group_name \
    --SubnetName $subnet_name \
    --VmSubnetName $vmsubnet_name \
    --BatchSubnetName $batchsubnet_name \
    --PostgreSqlSubnetName $sqlsubnet_name \
    --HelmBinaryPath /usr/local/bin/helm \
    --TesImageName $tes_image_name \
    --TriggerServiceImageName $trigger_service_image_name \
    --DebugLogging true"
