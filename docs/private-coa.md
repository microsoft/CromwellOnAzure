## Setting up private networking for Cromwell on Azure.

The following are instructions on how to setup a virtual network, and azure container registry to run Cromwell on Azure with private networking so that no components have public IP addresses, all traffic is routed through the virtual network or private endpoints. 


### 0. Set variables and create Resource group:

    ```
    subscription="594bef42-33f3-42df-b056-e7399d6ae7a0"
    resource_group_name=coajsaun123
    vnet_name=vnetjsaun456
    mycontainerregistry=jsauncontainerregistry007
    storage_account_name=jsaunstorageaccount123
    cosmos_db_name=coacosmosdbjsaun123
    private_endpoint_name_cosmos=myprivateendpoint123cosmos
    private_endpoint_name_storage=myprivateendpoint123storage
    private_endpoint_name_cr=myprivateendpoint123cr
    location=eastus
    failoverLocation=eastus2
    
    // Create Resource group
    az group create -n $resource_group_name -l $location
    ```

### 1. Provision a virtual network and subnets for CoA resources with an appropriate CIDR. 

    ```
    az network vnet create -g $resource_group_name -n $vnet_name --address-prefixes 10.1.0.0/16
    az network vnet subnet create -g $resource_group_name --vnet-name $vnet_name -n vmsubnet --address-prefixes 10.1.0.0/24
    az network vnet subnet create -g $resource_group_name --vnet-name $vnet_name -n mysqlsubnet --address-prefixes 10.1.1.0/24
    az network vnet subnet create -g $resource_group_name --vnet-name $vnet_name -n batchnodessubnet --address-prefixes 10.1.2.0/24
    az network vnet subnet create -g $resource_group_name --vnet-name $vnet_name -n pesubnet --address-prefixes 10.1.3.0/24

    az network vnet subnet update --resource-group $resource_group_name --vnet-name $vnet_name --name vmsubnet --service-endpoints "Microsoft.Storage"
    az network vnet subnet update --resource-group $resource_group_name --vnet-name $vnet_name --name mysqlsubnet --service-endpoints "Microsoft.Storage"
    az network vnet subnet update --resource-group $resource_group_name --vnet-name $vnet_name --name batchnodessubnet --service-endpoints "Microsoft.Storage"

    az network vnet subnet update \
        --name pesubnet \
        --resource-group $resource_group_name \
        --vnet-name $vnet_name \
        --disable-private-endpoint-network-policies true

    az network vnet subnet update \
        --name batchnodessubnet \
        --resource-group $resource_group_name \
        --vnet-name $vnet_name \
        --disable-private-link-service-network-policies true
    ```

### 2. Provision a VM to run the CoA deployer. Since the CoA VM will not have direct internet access, we need to create a temporary jumpbox on the virtal network with a public IP address so the deployer can have ssh access to the CoA VM.

    ```
    az vm create -n PrivateDeployerVM3 -g $resource_group_name --vnet-name $vnet_name --subnet vmsubnet --image UbuntuLTS --admin-username azureuser --generate-ssh-keys
    ```
    
### 3. SSH into the deployer VM we created

    ```
    ssh azureuser@{public-ip}
    
    
    // Install AZ CLI
    curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
    
     // Login into Az
    az login  
    
    az account set --subscription $subscription
    // Copy and paste all the environments variables such as $subscription from the other shell.
    batchsubnetid=$(az network vnet subnet show --resource-group $resource_group_name --vnet-name $vnet_name --name batchnodessubnet --query id --output tsv)
    ```
  

### 4. Create CosmosDb and Storage Account to be used by CoA without public access, and establish private endpoints with virtual network we already created.
    ```
    az storage account create --name $storage_account_name --resource-group $resource_group_name

    az storage account network-rule add --resource-group $resource_group_name --account-name $storage_account_name --subnet vmsubnet --vnet-name $vnet_name
    az storage account network-rule add --resource-group $resource_group_name --account-name $storage_account_name --subnet mysqlsubnet --vnet-name $vnet_name
    az storage account network-rule add --resource-group $resource_group_name --account-name $storage_account_name --subnet batchnodessubnet --vnet-name $vnet_name
    
    stroageAccountId="/subscriptions/$subscription/resourceGroups/$resource_group_name/providers/Microsoft.Storage/storageAccounts/$storage_account_name"
    MSYS_NO_PATHCONV=1 az network private-endpoint create \
            --name $private_endpoint_name_storage \
            --resource-group $resource_group_name \
            --vnet-name $vnet_name  \
            --subnet pesubnet \
            --private-connection-resource-id $stroageAccountId \
            --group-id "Blob" \
            --connection-name "myConnection"

    az cosmosdb create --name $cosmos_db_name --resource-group $resource_group_name \
        --default-consistency-level Eventual \
        --locations regionName="$location" failoverPriority=0 isZoneRedundant=False \
        --locations regionName="$failoverLocation" failoverPriority=1 isZoneRedundant=False

    cosmosId="/subscriptions/$subscription/resourceGroups/$resource_group_name/providers/Microsoft.DocumentDB/databaseAccounts/$cosmos_db_name"
    MSYS_NO_PATHCONV=1 az network private-endpoint create \
        --name $private_endpoint_name_cosmos \
        --resource-group $resource_group_name \
        --vnet-name $vnet_name  \
        --subnet pesubnet \
        --private-connection-resource-id $cosmosId \
        --group-id "Sql" \
        --connection-name "myConnection"

    zoneName="privatelink.documents.azure.com"

    az network private-dns zone create --resource-group $resource_group_name \
        --name  $zoneName

    az network private-dns link vnet create --resource-group $resource_group_name \
        --zone-name  $zoneName\
        --name myzonelink \
        --virtual-network $vnet_name \
        --registration-enabled false 

    az network private-endpoint dns-zone-group create \
        --resource-group $resource_group_name \
        --endpoint-name $private_endpoint_name_cosmos \
        --name "MyPrivateZoneGroup" \
        --private-dns-zone $zoneName \
        --zone-name "myzone" 
    ```

### 4. Provision Azure Container Registry

    ```
    // Instal docker if not on machine
    sudo apt  install docker.io
    
    az acr create --resource-group $resource_group_name --name $mycontainerregistry --sku Premium
    az acr login --name $mycontainerregistry
    
    
    // To account for dockerinDocker [issue 401](https://github.com/microsoft/CromwellOnAzure/issues/401)
    // create docker file 
    
    
    FROM docker.io/library/docker:latest
    LABEL author="Venkat S. Malladi"

    ENV DEBIAN_FRONTEND=noninteractive

    RUN apk add grep
    RUN apk add bash

    sudo docker build -t $mycontainerregistry.azurecr.io/docker:v1 .
    sudo docker push $mycontainerregistry.azurecr.io/docker:v1
    
    //az acr import \
    //  --name $mycontainerregistry \
     // --source docker.io/library/docker:latest \
     // --image docker:v1

    az acr import \
      --name $mycontainerregistry \
      --source mcr.microsoft.com/blobxfer \
      --image blobxfer:v1

    az acr import \
      --name $mycontainerregistry \
      --source mcr.microsoft.com/mirror/docker/library/ubuntu:22.04 \
      --image ubuntu:22.04
    
    az acr update --name $mycontainerregistry --public-network-enabled false

    
    acrID="/subscriptions/$subscription/resourceGroups/$resource_group_name/providers/Microsoft.ContainerRegistry/registries/$mycontainerregistry"
    MSYS_NO_PATHCONV=1 az network private-endpoint create \
    --name $private_endpoint_name_cr \
    --resource-group $resource_group_name \
    --vnet-name $vnet_name  \
    --subnet pesubnet \
    --private-connection-resource-id $acrID \
    --group-id "registry" \
    --connection-name "myConnection"
    
     zoneName="privatelink.azurecr.io"

    az network private-dns zone create --resource-group $resource_group_name \
        --name  $zoneName

    az network private-dns link vnet create --resource-group $resource_group_name \
        --zone-name  $zoneName\
        --name myzonelink \
        --virtual-network $vnet_name \
        --registration-enabled false 

    az network private-endpoint dns-zone-group create \
        --resource-group $resource_group_name \
        --endpoint-name $private_endpoint_name_cr \
        --name "MyPrivateZoneGroup" \
        --private-dns-zone $zoneName \
        --zone-name "myzone" 
    ```

### 5. Run the deployer.
    ```
    // Set environment variables for CoA
    version=3.1.0
    coa_identifier=vsmp
    
    //Download the installer
    wget https://github.com/microsoft/CromwellOnAzure/releases/download/$version/deploy-cromwell-on-azure-linux
    chmod 744 deploy-cromwell-on-azure-linux
   
    ./deploy-cromwell-on-azure-linux --SubscriptionId $subscription --RegionName $location \
        --MainIdentifierPrefix $coa_identifier \
        --StorageAccountName $storage_account_name \
        --CosmosDbAccountName $cosmos_db_name \
        --PrivateNetworking true \
        --BatchNodesSubnetId $batchsubnetid \
        --DisableBatchNodesPublicIpAddress true \
        --DockerInDockerImageName "$mycontainerregistry.azurecr.io/docker:v1" \
        --BlobxferImageName "$mycontainerregistry.azurecr.io/blobxfer:v1" \
        --ResourceGroupName $resource_group_name \
        --VnetName $vnet_name \
        --VnetResourceGroupName $resource_group_name \
        --VmSubnetName vmsubnet
    ```

### 6. Update wdl and Managed identity access to run test workflow
```
    // Add Network Contributor as Resource Group [issue 450](https://github.com/microsoft/CromwellOnAzure/issues/450)
    Add MI of CoA VM Network Contributor RBAC at the Resource Group it fixes this issues
    Restart CoA VM

//[Use private Docker containers hosted on Azure](https://github.com/microsoft/CromwellOnAzure/blob/develop/docs/troubleshooting-guide.md#use-private-docker-containers-hosted-on-azure)
    
    //Update path of container in test.wdl ($storage_account_name/inputs.test.wdl)  [issue 585](https://github.com/microsoft/CromwellOnAzure/issues/585)  
     docker: '$mycontainerregistry.azurecr.io/ubuntu:20.04'
     
     //Upload new trigger file to test.
     
     {
    "WorkflowUrl": "/$storage_account_name/inputs/test/test.wdl",
    "WorkflowInputsUrl": "/$storage_account_name/inputs/test/testInputs.json",
    "WorkflowInputsUrls": null,
    "WorkflowOptionsUrl": null,
    "WorkflowDependenciesUrl": null,
  }
    ```
    
    
