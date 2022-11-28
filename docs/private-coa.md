## Setting up private networking for Cromwell on Azure.

The following are instructions on how to setup a virtual network, and azure container registry to run Cromwell on Azure with private networking so that no components have public IP addresses, all traffic is routed through the virtual network or private endpoints. 


### 0. Set variables:

    ```
    subscription="594bef42-33f3-42df-b056-e7399d6ae7a0"
    resource_group_name=coajsaun123
    vnet_name=vnetjsaun456
    mycontainerregistry=jsauncontainerregistry007
    storage_account_name=jsaunstorageaccount123
    cosmos_db_name=coacosmosdbjsaun123
    private_endpoint_name_cosmos=myprivateendpoint123cosmos
    private_endpoint_name_storage=myprivateendpoint123storage
    location=eastus
    failoverLocation=eastus2
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

    batchsubnetid=$(az network vnet subnet show --resource-group $resource_group_name --vnet-name $vnet_name --name batchnodessubnet --query id --output tsv)
    ```

### 2. Provision a VM to run the CoA deployer. Since the CoA VM will not have direct internet access, we need to create a temporary jumpbox on the virtal network with a public IP address so the deployer can have ssh access to the CoA VM.

    ```
    az vm create -n PrivateDeployerVM3 -g $resource_group_name --vnet-name $vnet_name --subnet vmsubnet --image UbuntuLTS --admin-username azureuser --generate-ssh-keys
    ```

### 3. Create CosmosDb and Storage Account to be used by CoA without public access, and establish private endpoints with virtual network we already created.
    ```
    az storage account create --name $storage_account_name --resource-group $resource_group_name

    az storage account network-rule add --resource-group $resource_group_name --account-name $storage_account_name --subnet vmsubnet --vnet-name $vnet_name
    az storage account network-rule add --resource-group $resource_group_name --account-name $storage_account_name --subnet mysqlsubnet --vnet-name $vnet_name
    az storage account network-rule add --resource-group $resource_group_name --account-name $storage_account_name --subnet batchnodessubnet --vnet-name $vnet_name

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
    az acr create --resource-group $resource_group_name --name $mycontainerregistry --sku Basic
    az acr login --name $mycontainerregistry

    docker pull docker
    docker tag docker $mycontainerregistry.azurecr.io/docker:v1
    docker push $mycontainerregistry.azurecr.io/docker:v1
    docker rmi $mycontainerregistry.azurecr.io/docker:v1

    docker pull mcr.microsoft.com/blobxfer
    docker tag mcr.microsoft.com/blobxfer $mycontainerregistry.azurecr.io/blobxfer:v1
    docker push $mycontainerregistry.azurecr.io/blobxfer:v1
    docker rmi $mycontainerregistry.azurecr.io/blobxfer:v1
    ```

### 5. SSH into the deployer VM we created, and run the deployer.
    ```
    ssh azureuser@{public-ip}
    
    version=3.1.0
    curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
    wget https://github.com/microsoft/CromwellOnAzure/releases/download/$version/deploy-cromwell-on-azure-linux
    
    // Copy and paste all the environments variables such as $subscription from the other shell.
    batchsubnetid=$(az network vnet subnet show --resource-group $resource_group_name --vnet-name $vnet_name --name batchnodessubnet --query id --output tsv)

    ./deploy-cromwell-on-azure-linux --subscriptionid $subscription --regionname japaneast \
        --mainidentifierprefix coajsaunmain \
        --StorageAccountName $storage_account_name \
        --CosmosDbAccountName $cosmos_db_name \
        --privatenetworking true \
        --BatchNodesSubnetId $batchsubnetid \
        --DisableBatchNodesPublicIpAddress true \
        --DockerInDockerImageName "$mycontainerregistry.azurecr.io/docker" \
        --BlobxferImageName "$mycontainerregistry.azurecr.io/blobxfer" \
        --resourcegroupname $resource_group_name \
        --vnetname $vnet_name \
        --vnetresourcegroupname $resource_group_name \
        --subnetname vmsubnet
    ```



Task e5eee20a_6920ab86dc374c6b8c66762b94736543 failed. ExitCode: 1, BatchJobInfo: {"MoreThanOneActiveJobFound":false,"ActiveJobWithMissingAutoPool":false,"AttemptNumber":1,"NodeAllocationFailed":false,"NodeErrorCode":null,"NodeErrorDetails":null,"JobState":0,"JobStartTime":"2022-06-10T05:38:25.2447107Z","JobEndTime":null,"JobSchedulingError":null,"NodeState":0,"TaskState":3,"TaskExitCode":1,"TaskExecutionResult":1,"TaskStartTime":"2022-06-10T05:41:07.237451Z","TaskEndTime":"2022-06-10T05:41:24.968525Z","TaskFailureInformation":{"Category":0,"Code":"FailureExitCode","Details":[{"Name":"Message","Value":"The task process exited with an unexpected exit code"},{"Name":"AdditionalErrorCode","Value":"FailureExitCode"}],"Message":"The task exited with an exit code representing a failure"},"TaskContainerState":null,"TaskContainerError":null}
