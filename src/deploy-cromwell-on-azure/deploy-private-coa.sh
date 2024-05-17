#!/bin/bash
set -e
set -o pipefail

# This script deploys a private instance of Cromwell on Azure (CoA) within a specified Azure subscription
# and location. It includes the setup of a virtual network, an Azure Firewall, and a VM jumpbox, ensuring
# a secure environment for running Cromwell on Azure.

# Usage: deploy-private-coa.sh <subscription> <location> <prefix> <azure_cloud_name>

# Ensure jq and .NET 8 are installed
if ! command -v jq &>/dev/null; then
    echo "jq is not installed. Installing jq..."
    sudo apt-get update && sudo apt-get install -y jq
fi

if ! dotnet --list-sdks | grep -q '8\.'; then
    echo ".NET 8 SDK is not installed. Installing .NET 8 SDK..."
    wget https://dot.net/v1/dotnet-install.sh
    chmod +x dotnet-install.sh
    ./dotnet-install.sh --version 8.0.100
    # Reload the environment to ensure dotnet is in PATH
    source ~/.profile
fi

prefix="coa"
azure_cloud_name="azurecloud"

if [ $# -lt 2 ]; then
    echo "Usage: $0 <subscription> <location> [prefix] [azure_cloud_name]"
    echo "Note: [prefix] defaults to 'coa' and [azure_cloud_name] defaults to 'azurecloud' if not provided."
    exit 1
fi

subscription=$1
location=$2
prefix=${3:-$prefix}
azure_cloud_name=${4:-$azure_cloud_name}

### VARIABLES
# Network
hub_vnet_cidr="10.0.0.0/16"
hub_subnet_cidr="10.0.1.0/24"
spoke0_vnet_cidr="10.100.0.0/16"
spoke0_subnet_cidr="10.100.0.0/24"
#firewall_subnet_cidr="10.100.1.0/24"
aks_subnet_cidr="10.100.1.0/24"
psql_subnet_cidr="10.100.2.0/24"
kubernetes_service_cidr="10.100.3.0/24"
kubernetes_dns_ip="10.100.3.10"
batch_subnet_cidr="10.100.128.0/17"
# Resource names
resource_group_name="${prefix}-main"
hub_vnet_name="${prefix}-hub-vnet"
hub_subnet_name="AzureFirewallSubnet" # firewall will be placed in hub subnet
spoke0_vnet_name="${prefix}-spoke0-vnet"
spoke0_subnet_name="${prefix}-spoke-subnet"
deployer_subnet_name="${prefix}-deployer-subnet"
#firewall_subnet_name="AzureFirewallSubnet" # "${prefix}-firewall-subnet"
route_table_name="${prefix}-route-table"
firewall_name="${prefix}-firewall"
dns_zone_name="${prefix}.private.${location}.azmk8s.io"
tes_image_name="mcr.microsoft.com/CromwellOnAzure/tes:5.3.1.12044"
trigger_service_image_name="mcr.microsoft.com/CromwellOnAzure/triggerservice:5.3.1.12044"
temp_deployer_vm_name="${prefix}-coa-deploy"
coa_binary_path="/tmp/coa"
coa_binary="deploy-cromwell-on-azure"

create_resource_group_if_not_exists() {
  local rg_name=$1
  local rg_location=$2

  if [ $(az group exists --name "$rg_name") = false ]; then
    echo "Creating resource group '$rg_name' in location '$rg_location'."
    az group create --name "$rg_name" --location "$rg_location"
  else
    echo "Resource group '$rg_name' already exists."
  fi
}

rm -f ../ga4gh-tes/nuget.config

if [ -f "./deploy-cromwell-on-azure-linux" ]; then
    coa_binary="deploy-cromwell-on-azure-linux"
elif [ -f "./deploy-cromwell-on-azure" ]; then
    coa_binary="deploy-cromwell-on-azure"
else
    # publish a new deployer binary
    echo "Building the deployer..."
    dotnet publish -r linux-x64 -c Release -o ./ /p:PublishSingleFile=true /p:DebugType=none /p:IncludeNativeLibrariesForSelfExtract=true
fi

create_resource_group_if_not_exists $resource_group_name $location

echo "Creating Hub virtual network..."
az network vnet create \
    --resource-group $resource_group_name \
    --name $hub_vnet_name \
    --address-prefixes $hub_vnet_cidr \
    --subnet-name $hub_subnet_name \
    --subnet-prefixes $hub_subnet_cidr
 
echo "Creating spoke0 virtual network..."

az network vnet create \
    --resource-group $resource_group_name \
    --name $spoke0_vnet_name \
    --address-prefixes $spoke0_vnet_cidr \
    --subnet-name $spoke0_subnet_name \
    --subnet-prefixes $spoke0_subnet_cidr

az network vnet peering create --name HubToSpoke0 --resource-group $resource_group_name --vnet-name $hub_vnet_name --remote-vnet $spoke0_vnet_name --allow-vnet-access
az network vnet peering create --name Spoke0ToHub --resource-group $resource_group_name --vnet-name $spoke0_vnet_name --remote-vnet $hub_vnet_name --allow-vnet-access

echo "Creating AKS private DNS zone..."
dns_zone_id=$(az network private-dns zone create --resource-group $resource_group_name --name $dns_zone_name --query "id" -o tsv)
az network private-dns link vnet create --resource-group $resource_group_name --zone-name $dns_zone_name --name "${spoke0_vnet_name}-dns-link" --virtual-network $spoke_vnet_name --registration-enabled false

#echo "Creating firewall subnet..."
#az network vnet subnet create --resource-group $resource_group_name --vnet-name $vnet_name --name $firewall_subnet_name --address-prefixes $firewall_subnet_cidr

echo "Creating public IP for Azure Firewall..."
az network public-ip create --name "${firewall_name}-pip" --resource-group $resource_group_name --location $location --sku "Standard" --allocation-method "Static"

echo "Creating Azure Firewall..."
az network firewall create --name $firewall_name --resource-group $resource_group_name --location $location --vnet-name $hub_vnet_name

firewall_public_ip_id=$(az network public-ip show --name "${firewall_name}-pip" --resource-group $resource_group_name --query "id" -o tsv)

echo "Started at $(date '+%I:%M:%S%p'): Creating firewall IP configuration (takes 10-30 minutes)..."
az network firewall ip-config create --firewall-name $firewall_name --name "${firewall_name}-config" --public-ip-address $firewall_public_ip_id --resource-group $resource_group_name --vnet-name $hub_vnet_name --subnet $hub_subnet_name
firewall_private_ip=$(az network firewall show --name $firewall_name --resource-group $resource_group_name | jq -r '.ipConfigurations[0].privateIPAddress')

echo "Creating route table..."
az network route-table create --name $route_table_name --resource-group $resource_group_name --location $location

echo "Creating route to direct AKS and Batch subnet Internet traffic through the Azure Firewall..."
az network route-table route create --name "route-to-firewall" --route-table-name $route_table_name --resource-group $resource_group_name --address-prefix "0.0.0.0/0" --next-hop-type "VirtualAppliance" --next-hop-ip-address $firewall_private_ip

echo "Creating subnets..."
az network vnet subnet create --resource-group $resource_group_name --vnet-name $spoke0_vnet_name -n aks-subnet --address-prefixes $aks_subnet_cidr --route-table $route_table_name
az network vnet subnet create --resource-group $resource_group_name --vnet-name $spoke0_vnet_name -n psql-subnet --address-prefixes $psql_subnet_cidr
az network vnet subnet create --resource-group $resource_group_name --vnet-name $spoke0_vnet_name -n batch-subnet --address-prefixes $batch_subnet_cidr --route-table $route_table_name

az network vnet subnet update --resource-group $resource_group_name --vnet-name $spoke0_vnet_name --name aks-subnet --service-endpoints "Microsoft.Storage"
az network vnet subnet update --resource-group $resource_group_name --vnet-name $spoke0_vnet_name --name batch-subnet --service-endpoints "Microsoft.Storage"
az network vnet subnet update --name psql-subnet --resource-group $resource_group_name --vnet-name $spoke0_vnet_name --disable-private-endpoint-network-policies true
az network vnet subnet update --name batch-subnet --resource-group $resource_group_name --vnet-name $spoke0_vnet_name --disable-private-link-service-network-policies true

deployer_subnet_id=$(az network vnet subnet show --resource-group $resource_group_name --vnet-name $spoke0_vnet_name --name $deployer_subnet_name --query "id" -o tsv)

echo "Creating VM jumpbox within the virtual network to deploy from..."
vm_public_ip=$(az vm create --resource-group $resource_group_name --name $temp_deployer_vm_name --image Ubuntu2204 --admin-username azureuser --generate-ssh-keys --subnet $deployer_subnet_id --query publicIpAddress -o tsv)

echo "Opening port 22 for SSH access..."
az vm open-port --port 22 --resource-group $resource_group_name --name $temp_deployer_vm_name

echo "Waiting for port to open..."
sleep 10

echo "Installing AZ CLI and logging in..."
ssh -o StrictHostKeyChecking=no azureuser@$vm_public_ip "curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash; az cloud set -n $azure_cloud_name; az login --use-device-code"

echo "Creating directory..."
ssh -o StrictHostKeyChecking=no azureuser@$vm_public_ip "mkdir -p /tmp/coa"

echo "Copying CoA deployment binary..."
scp -o StrictHostKeyChecking=no $coa_binary azureuser@$vm_public_ip:/tmp/coa/$coa_binary

echo "Installing Helm..."
ssh -o StrictHostKeyChecking=no azureuser@$vm_public_ip "curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash"

echo "Setting CoA deployer binary to executable..."
ssh -o StrictHostKeyChecking=no azureuser@$vm_public_ip "chmod +x $coa_binary_path/$coa_binary"

echo "Executing CoA deployer binary..."
ssh -o StrictHostKeyChecking=no azureuser@$vm_public_ip "$coa_binary_path/$coa_binary \
    --SubscriptionId $subscription \
    --ResourceGroupName $resource_group_name \
    --RegionName $location \
    --AzureCloudName $azure_cloud_name \
    --MainIdentifierPrefix $prefix \
    --PrivateNetworking true \
    --DisableBatchNodesPublicIpAddress true \
    --VnetName $spoke_vnet_name \
    --VnetResourceGroupName $resource_group_name \
    --VmSubnetName aks-subnet \
    --PostgreSqlSubnetName psql-subnet \
    --BatchSubnetName batch-subnet \
    --HelmBinaryPath /usr/local/bin/helm \
    --TesImageName $tes_image_name \
    --TriggerServiceImageName $trigger_service_image_name \
    --DebugLogging true \
    --KubernetesServiceCidr $kubernetes_service_cidr \
    --KubernetesDnsServiceIp $kubernetes_dns_ip \
    --UserDefinedRouting true \
    --AksPrivateDnsZoneResourceId $dns_zone_id"
