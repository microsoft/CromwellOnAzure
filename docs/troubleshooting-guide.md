# FAQs, advanced troubleshooting and known issues for Cromwell on Azure

This article answers FAQs, describes advanced features that allow customization and debugging of Cromwell on Azure, as well as how to diagnose, debug, and work around [known issues](#Known-Issues-And-Mitigation). We are actively tracking these as bugs to be fixed in upcoming releases!

1. Setup
   * I am trying to [setup Cromwell on Azure for multiple users on the same subscription](#Setup-Cromwell-on-Azure-for-multiple-users-in-the-same-Azure-subscription), what are the ways I can do this?
   * I ran the [Cromwell on Azure installer and it failed](#Debug-my-Cromwell-on-Azure-installation-that-ran-into-an-error). How can I fix it?
   * How can I [upgrade my Cromwell on Azure instance?](#Upgrade-my-Cromwell-on-Azure-instance)

2. Analysis
   * I submitted a job and it failed almost immediately. What [happened to it](#Job-failed-immediately)?
   * I can only run small workflows but not workflows that require multiple tasks with a large total cpu cores requirement. [How do I increase workflows capacity?](#Check-Azure-Batch-account-quotas)
   * How do I [setup my own WDL](#Set-up-my-own-WDL) to run on Cromwell?
   * How can I see [how far along my workflow has progressed?](#Check-all-tasks-running-for-a-workflow-using-Batch-account)
   * My workflow failed at task X. Where should I look to determine why it failed?
     * [Which tasks failed?](#Find-which-tasks-failed-in-a-workflow)
     * Some tasks are stuck or my workflow is stuck in the "inprogress" directory in the "workflows" container. [Were there Azure infrastructure issues?](#Make-sure-there-are-no-Azure-infrastructure-errors)
   * My jobs are taking a long time in the "Preparing" task state, even with "smaller" input files and VMs being used. [Why is that](#Check-Azure-Storage-Tier)?

3. Customizing your instance
   * How can I [customize my Cromwell on Azure deployment?](#customize-your-cromwell-on-azure-deployment)
   * How can I [use a specific Cromwell image version?](#use-a-specific-cromwell-version)
   * How do I [use input data files for my workflows from a different Azure Storage account](#use-input-data-files-from-an-existing-azure-storage-account-that-my-lab-or-team-is-currently-using) that my lab or team is currently using?
   * Can I connect a different [batch account with previously increased quotas](#use-a-batch-account-for-which-i-have-already-requested-or-received-increased-cores-quota-from-azure-support) to run my workflows?
   * How can I [use private Docker containers for my workflows?](#use-private-docker-containers-hosted-on-azure)
   * A lot of tasks for my workflows run longer than 24 hours and have been randomly stopped. How can I [run all my tasks on dedicated batch VMs?](#configure-my-cromwell-on-azure-instance-to-always-use-dedicated-batch-vms-to-avoid-getting-preempted)
   * Can I get [direct access to Cromwell's REST API?](#access-the-cromwell-rest-api-directly-from-linux-host-vm)

4. Performance & Optimization
   * How can I figure out how much Cromwell on Azure costs me?
     * How much am I [paying for my Cromwell on Azure instance?](#Cost-analysis-for-Cromwell-on-Azure)
     * How are [batch VMs selected to run tasks in a workflow?](#How-Cromwell-on-Azure-selects-batch-VMs-to-run-tasks-in-a-workflow)
   * Do you have guidance on how to [optimize my WDLs](#Optimize-my-WDLs)?

5. Miscellaneous
   * I cannot find my issue in this document and [want more information](#Get-container-logs-to-debug-issues) from Cromwell, MySQL, or TES Docker container logs.
   * I am running a large number of workflows and [MySQL storage disk is full](#I-am-running-a-large-number-of-workflows-and-MySQL-storage-disk-is-full)
   * How can I run [CWL](#Running-CWL-Workflows-on-Cromwell-on-Azure) files on Cromwell on Azure?


## Known Issues and Mitigation
### I am trying to use files with SAS tokens but run into file access issues
There is currently a bug (which we are tracking) in a dependency tool we use to get files from Azure Storage to the VM to perform a task. For now, follow these steps as a workaround if you are running into errors getting access to your files using SAS tokens on Cromwell on Azure.
If you followed [these](https://docs.microsoft.com/en-us/azure/storage/common/storage-sas-overview#get-started-with-sas) instructions to create a SAS URL, you’ll get something similar to
```
https://YourStorageAccount.blob.core.windows.net/inputs?sv=2018-03-28si=inputs-key&sr=c&sig=somestring
```

Focus on this part: **si=inputs-key&sr=c** <br/>

Manually change order of `sr` and `si` fields to get something similar to 
```
https://YourStorageAccount.blob.core.windows.net/inputs?sv=2018-03-28&sr=c&si=inputs-keysig=somestring
```

After the change, **sr=c&si=inputs-key** should be the order in your SAS URL. <br/>

Update all the SAS URLs similarly and retry your workflow.

### All TES tasks for my workflow are done running, but the trigger JSON file is still in the "inprogress" directory in the workflows container
1. The root cause is most likely memory pressure on the host Linux VM because [blobfuse](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-how-to-mount-container-linux#overview) processes grow to consume all physical memory. 

You may see the following Cromwell container logs as a symptom:
> Cromwell shutting down because it cannot access the database):
Shutting down cromid-5bd1d24 as at least 15 minutes of heartbeat write errors have occurred between 2020-02-18T22:03:01.110Z and 2020-02-18T22:19:01.111Z (16.000016666666667 minutes

To mitigate, please resize your VM in the resource group to a machine with at least 14GB memory/RAM. Any workflows still in progress will not be affected.

![Resize VM](/docs/screenshots/resize-VM.png)

2. Another possible scenario is that the "mysql" database is in an unusable state, which means Cromwell cannot continue processing workflows.

You may see the following Cromwell container logs as a symptom:
> Failed to instantiate Cromwell System. Shutting down Cromwell.
liquibase.exception.LockException: Could not acquire change log lock.  Currently locked by 012ec19c3285 (172.18.0.4) since 2/19/20 4:10 PM

**Note: This has been fixed in Release 2.1. If you use the 2.1 deployer or update to this version, you can skip the mitigation steps below**

For Release 2.0 and below:
To mitigate, log on to the host VM and execute the following and then restart the VM:

```
sudo docker exec -it cromwellazure_mysqldb_1 bash -c 'mysql -ucromwell -Dcromwell_db -pcromwell -e"SELECT * FROM DATABASECHANGELOGLOCK;UPDATE DATABASECHANGELOGLOCK SET LOCKED=0, LOCKGRANTED=null, LOCKEDBY=null where ID=1;SELECT * FROM DATABASECHANGELOGLOCK;"'
```

### Trigger JSON file for my workflow stays in the "new" directory in the workflows container and no task is started
The root cause is most likely failing MySQL upgrade. 

You may see the following Cromwell container logs:
> Failed to instantiate Cromwell System. Shutting down Cromwell.
java.sql.SQLTransientConnectionException: db - Connection is not available, request timed out after 15000ms.

and MySQL container logs as a symptom:
> Upgrade is not supported after a crash or shutdown with innodb_fast_shutdown = 2. 

To mitigate, follow instructions on [MySQL update](mysql-update.md).

## Setup
### Setup Cromwell on Azure for multiple users in the same Azure subscription
Cromwell on Azure is designed to be flexible for single and multiple user scenarios. Here we have envisioned four general scenarios and demonstrated how they relate to your Azure account, Azure Batch service, Subscription ID, and Resource Groups, each depicted below.

![Multiple Users FAQ](/docs/screenshots/multiple-users-configs.png)

1) **The Individual User**: This is the current standard deployment configuration for Cromwell on Azure. No extra steps beyond the [deployment guide](../README.md/#deploy-your-instance-of-cromwell-on-azure) are necessary.

2) **The Lab**: This scenario is envisioned for small lab groups and teams sharing a common Azure resource (i.e. a common bioinformatician(s), data scientist(s), or computational biologist(s) collaborating on projects from the same lab). Functionally, this setup does not differ from the "Individual User" configuration. We recommend a single "Cromwell Administrator" perform the initial Cromwell on Azure  setup for the group. Ensure that this user has the appropriate role(s) on the Subscription ID as outlined [here](../README.md/#Prerequisites). Once deployed, this "Cromwell Administrator" can [grant "Contributor" access to the created Cromwell storage account via the Azure Portal](https://docs.microsoft.com/en-us/azure/storage/common/storage-auth-aad-rbac-portal#assign-an-azure-built-in-role). This would allow granted users the ability to submit analysis jobs and retrieve results. It would also allow them the ability to view *any analysis* that has been run by the lab. As Cromwell submits all jobs to Azure Batch as one user, the billing for Cromwell on Azure usage would be collective for the entire lab, not broken down by individual users who submitted the jobs. 

3) **The Research Group**: This scenario is envisioned for larger research groups where a common Azure subscription is shared, but users want/require their own instance of Cromwell on Azure. The initial Cromwell on Azure deployment is done as described in the [deployment guide](../README.md/#deploy-your-instance-of-cromwell-on-azure). After the first deployment of Cromwell on Azure is done on the Subscription, subsequent users will need to specify a *separate Resource Group* **AND** *preexisting Azure Batch account name* that is currently being utilized by the pre-existing deployment(s) of Cromwell on Azure. The Azure Batch account must exist in the same region as defined in the "--RegionName" configuration of the new Cromwell on Azure deployment. You can check all the [configuration options here](#Customize-your-Cromwell-on-Azure-deployment). See the invocation of the Linux deployment script for an example: 

```
.\deploy-cromwell-on-azure-linux --SubscriptionId <Your subscription ID> --RegionName <Your region> --MainIdentifierPrefix <Your string> --ResourceGroupName <Your resource group> --BatchAccountName <Your Batch account name>
```

   In this scenario, please note the lack of separation at the Azure Batch account level. While you will be able track resource usage independently due to the separate Cromwell users submitting analyses to Azure Batch (for your own tracking/internal billing purposes), anyone who has access to Azure Batch as a Contributor or Owner will be able to see ***everyone's*** Batch pools, and thus what they are running. For this scenario, we would recommend the Cromwell Administrator(s) be trusted personnel, such as your IT team.

4) **The Institution**: This is an enterprise level deployment scenario for a large organization with multiple Subscriptions and independent user groups within an internal hierarchy. In this scenario, due to the independent nature of the work being done and the desire/need to track specific resource usage (for your own internal billing purposes) you will have ***completely independent*** deployments of Cromwell on Azure.  

   To deploy, you'll need to verify whether an existing Azure Batch account already exists on your Subscription (to run Cromwell on Azure on the Subscription level), or within your Resource Group as described in the [deployment guide](../README.md/#deploy-your-instance-of-cromwell-on-azure), with appropriate [roles](https://docs.microsoft.com/en-us/azure/role-based-access-control/built-in-roles) set. If Azure Batch account is not deployed on your Subscription (or if you have available quota to create a new Batch account - the default for most accounts is 1 Batch account/region), then simply follow the [deployment guide](../README.md/#deploy-your-instance-of-cromwell-on-azure). If there is an existing Azure Batch account you're connecting to within your Subscription, simply follow the deployment recommendations outlined in [3], adding the appropriate flags for the deployment script. See the invocation of the Linux deployment script for an example: 

```
.\deploy-cromwell-on-azure-linux --SubscriptionId <Your subscription ID> --RegionName <Your region> --MainIdentifierPrefix <Your string> --ResourceGroupName <Your resource group> --BatchAccountName <Your Batch account name>
```

Please note you can also mix scenarios 1, 2, and 3 within the Azure Enterprise Account in scenario 4. 

### Debug my Cromwell on Azure installation that ran into an error
When the Cromwell on Azure installer is run, if there are errors, the logs are printed in the terminal. Most errors are related to insufficient permissions to create resources in Azure on your behalf, or intermittent Azure failures. In case of an error, we terminate the installation process and begin deleting all the resources in the Resource Group if already created. <br/>

Deleting all the resources in the Resource Group may take a while but as soon as you see logs that the batch account was deleted, you may exit the current process using Ctrl+C or Command+C on terminal/command prompt/PowerShell. The deletion of other Azure resources can continue in the background on Azure. Re-run the installer after fixing any user errors like permissions from the previous try.

If you see an issue that is unrelated to your permissions, and re-trying the installer does not fix it, please file a bug on our GitHub issues.

### Upgrade my Cromwell on Azure instance
Starting in version 1.x, for convenience, some configuration files are hosted in your Cromwell on Azure storage account, in the "configuration" container, for example `containers-to-mount`, `cromwell-application.conf`, and `allowed-vm-sizes`. You can modify and save these files using Azure Portal UI "Edit Blob" option or simply upload a new file to replace the existing one. Follow the instructions in the latest release to upgrade your Cromwell on Azure instance.

## Analysis
### Job failed immediately
If a workflow you start has a task that failed immediately and lead to workflow failure be sure to check your input JSON files. Follow the instructions [here](managing-your-workflow.md/#Configure-your-Cromwell-on-Azure-workflow-files) and check out an example WDL and inputs JSON file [here](example-fastq-to-ubam.md/#Configure-your-Cromwell-on-Azure-trigger-JSON,-inputs-JSON-and-WDL-files) to ensure there are no errors in defining your input files.

> For files hosted on an Azure Storage account that is connected to your Cromwell on Azure instance, the input path consists of three parts - the storage account name, the blob container name, file path with extension, following this format:
```
/<storageaccountname>/<containername>/<blobName>
```

> Example file path for an "inputs" container in a storage account "msgenpublicdata" will look like
`"/msgenpublicdata/inputs/chr21.read1.fq.gz"`

Another possibility is that you are trying to use a storage account that hasn't been mounted to your Cromwell on Azure instance - either by [default during setup](../README.md/#Cromwell-on-Azure-deployed-resources) or by following these steps to [mount a different storage account](#use-input-data-files-from-an-existing-azure-storage-account-that-my-lab-or-team-is-currently-using). <br/>

Check out these [known issues and mitigation](#Known-Issues-and-Mitigation) for more commonly seen issues caused by bugs we are actively tracking.

### Check Azure Batch account quotas
If you are running a task in a workflow with a large cpu cores requirement, check if your [Batch account has enough resource quotas](https://docs.microsoft.com/en-us/azure/batch/batch-quota-limit#resource-quotas). You can request more quotas by following [these instructions](https://docs.microsoft.com/en-us/azure/batch/batch-quota-limit#increase-a-quota).

For other resource quotas, like active jobs or pools, if there are not enough resources available, Cromwell on Azure keeps the tasks in queue until resources become available.
This may lead to longer wait times for workflow completion.

### Set up my own WDL
To get started you can view this [Hello World sample](../README.md/#Hello-World-WDL-test), an [example WDL](example-fastq-to-ubam.md) to convert FASTQ to UBAM or [follow these steps](change-existing-WDL-for-Azure.md) to convert an existing public WDL for other clouds to run on Azure. <br/>
There are also links to ready-to-try WDLs for common workflows [here](../README.md/#Run-Common-Omics-Workflows)

**Instructions to write a WDL file for a pipeline from scratch are COMING SOON.**

### Check all tasks running for a workflow using batch account
Each task in a workflow starts an Azure Batch VM. To see currently active tasks, navigate to your Azure Batch account connected to Cromwell on Azure on Azure Portal. Click on "Jobs" and then search for the Cromwell `workflowId` to see all tasks associated with a workflow.<br/>

![Batch account](screenshots/batch-account.png)

### Find which tasks failed in a workflow
[Cosmos DB](https://azure.microsoft.com/en-us/services/cosmos-db/) stores information about all tasks in a workflow. For monitoring or debugging any workflow you may choose to query the database.<br/>

Navigate to your Cosmos DB instance on Azure Portal. Click on the "Data Explorer" menu item, click on the "TES" container, and select "Items". <br/>

![Cosmos DB SQL query](screenshots/cosmosdb.PNG)

You can write a [SQL query](https://docs.microsoft.com/en-us/azure/cosmos-db/tutorial-query-sql-api) to get all tasks that have not completed successfully in a workflow using the following query, replacing `workflowId` with the id returned from Cromwell for your workflow:
```
SELECT * FROM c where startswith(c.description,"workflowId") AND c.state != "COMPLETE"
```

OR
```
SELECT * FROM c where startswith(c.id,"<first 9 character of the workflowId>") AND c.state != "COMPLETE"
```

### Make sure there are no Azure infrastructure errors
When working with Cromwell on Azure, you may run into issues with Azure Batch or Storage accounts. For instance, if a file path cannot be found or if the WDL workflow failed for an unknown reason. For these scenarios, consider debugging or collecting more information using [Application Insights](https://docs.microsoft.com/en-us/azure/azure-monitor/app/app-insights-overview).<br/>

Navigate to your Application Insights instance on Azure Portal. Click on the "Logs (Analytics)" menu item under the "Monitoring" section to get all logs from Cromwell on Azure's TES backend.<br/>

![App insights](screenshots/appinsights.PNG)

You can explore exceptions or logs to find the reason for failure and use time ranges or [Kusto Query Language](https://docs.microsoft.com/en-us/azure/kusto/query/) to narrow your search.<br/>

### Check Azure Storage Tier
Cromwell utilizes Blob storage containers and Blobfuse to allow your data to be accessed and processed. The [Blob Storage Access Tier](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blob-storage-tiers?tabs=azure-portal) can have a demonstrable effect on your analysis time, particularly on your initial VM preparation. If you experience this, we recommend setting your access tier to "Hot" instead of "Cool". You can do this under the "Access Tier" settings in the "Configuration" menu on Azure Portal. NOTE: this only affects users utilizing Gen2 Storage Accounts. All Gen 1 "Standard" blobs are access tier "Hot" by default.

## Customizing your Cromwell on Azure instance
### Connect to the host VM that runs all the Docker containers
To get logs from all the Docker containers or to use the Cromwell REST API endpoints, you may want to connect to the Linux host VM. At installation, a user is created to allow managing the host VM with username "vmadmin". The password is randomly generated and shown during installation. If you need to reset your VM password, you can do this using the Azure Portal or by following these [instructions](https://docs.microsoft.com/en-us/azure/virtual-machines/troubleshooting/reset-password). 

![Reset password](/docs/screenshots/resetpassword.PNG)

Starting with Release 3.0, unless you used the `--KeepSshPortOpen true` deployer option, in order to connect you can either
1. Enable and use the Azure-provided `just-in-time` feature (if that option is available to you in your subscription and for the host VM) OR
2. Change the `Action` of the `SSH` inbound security rule on the network security group in the VM's resource group from `Deny` to `Allow`. Be sure to switch it back to `Deny` when you log out.

To connect to your host VM, you can either
1. Construct your ssh connection string if you have the VM name `ssh vmadmin@<hostname>` OR
2. Navigate to the Connect button on the Overview blade of your Azure VM instance, then copy the ssh connection string. 

Paste the ssh connection string in a command line, PowerShell, or terminal application to log in.

![Connect with SSH](/docs/screenshots/connectssh.PNG)

### Customize your Cromwell on Azure deployment
Before deploying, you can choose to customize some input parameters to use existing Azure resources. Example:

```
.\deploy-cromwell-on-azure.exe --SubscriptionId <Your subscription ID> --RegionName <Your region> --MainIdentifierPrefix <Your string> --VmSize "Standard_D2_v2"
```

Here is the summary of common configuration parameters:

Configuration   parameter | Has default | Validated | Used by update | Comment
-- | -- | -- | -- | --
string   SubscriptionId | N | Y | Y | Azure Subscription Id - Always required
string   RegionName | N | Y | N | Azure region name to deploy to - Required for new install
string   MainIdentifierPrefix = "coa" | Y | Y | N | Prefix for all resources to be deployed - Required to deploy but defaults to "coa"
string   VmOsProvider = "Canonical" | Y | N | N | OS Provider VM to use as the host - Not required and defaults to Ubuntu
string   VmOsName = "UbuntuServer" | Y | N | N | OS Name of the VM to use as the host - Not required and defaults to UbuntuServer
string   VmOsVersion = "18.04-LTS" | Y | N | N | OS Version of the Linux Ubuntu VM to use as the host - Not required and defaults to Ubuntu 18.04 LTS
string   VmSize   = "Standard_D3_v2" | Y | N | N | VM size of the Linux Ubuntu VM to use as the host - Not required and defaults to [Standard_D3_v2](https://docs.microsoft.com/en-us/azure/cloud-services/cloud-services-sizes-specs#dv2-series)
string   VmUsername = "vmadmin"; | Y | N | Y | Username created on Cromwell on Azure Linux host - Not required and defaults to "vmadmin"
string   VmPassword | Y | N | Y | Required for update
string   VnetResourceGroupName | Y | Y | N | Available starting version 2.1. The resource group name of the specified virtual network to use - Not required, generated automatically if not provided. If specified, VnetName and SubnetName must be provided.
string   VnetName | Y | Y | N | Available starting version 2.1. The name of the specified virtual network to use - Not required, generated automatically if not provided. If specified, VnetResourceGroupName and SubnetName must be provided.
string   SubnetName | Y | Y | N | Available starting version 2.1. The subnet name of the specified virtual network to use - Not required, generated automatically if not provided. If specified, VnetResourceGroupName and VnetName must be provided.
string   VmSubnetName | Y | Y | N | Available starting version 3.1. The subnet name of the specified virtual network to use for the VM - Not required, generated automatically if not provided and ProvisionPostgreSqlOnAzure is true. If specified, VnetResourceGroupName, VnetName, and PostgreSqlSubnetName  must be provided.
string   PostgreSqlSubnetName  | Y | Y | N | Available starting version 3.1. The subnet name of the specified virtual network to use for the Azure PostgreSQL database- Not required, generated automatically if not provided and ProvisionPostgreSqlOnAzure is true. If specified, VnetResourceGroupName, VnetName, and VmSubnetName must be provided.
string   ResourceGroupName | Y | Y | Y | Required for update.   If provided for new Cromwell on Azure deployment, it must already exist.
string   BatchAccountName | Y | N | N | The name of the Azure Batch Account to use ; must be in the SubscriptionId and RegionName provided - Not required, generated automatically if not provided
string   StorageAccountName | Y | N | N | The name of the Azure Storage Account to use ; must be in the SubscriptionId provided - Not required, generated automatically if not provided
string   NetworkSecurityGroupName | Y | N | N | The name of the Network Security Group to use; must be in the SubscriptionId provided - Not required, generated automatically if not provided
string   CosmosDbAccountName | Y | N | N | The name of the Cosmos Db Account to use; must be in the SubscriptionId provided - Not required, generated automatically if not provided
string   ApplicationInsightsAccountName | Y | N | N | The name of the Application Insights Account to use; must be in the SubscriptionId provided - Not required, generated automatically if not provided
string   VmName | Y | N | Y | Name of the VM host that is part of the Cromwell on Azure deployment to update - Required for update if multiple VMs exist in the resource group
string   CromwellVersion | Y | N | Y | Cromwell version to use
bool     SkipTestWorkflow = false; | Y | Y | Y | Set to true to skip running the default [test workflow](../README.md/#Hello-World-WDL-test)
bool     Update =   false; | Y | Y | Y | Set to true if you want to update your existing Cromwell on Azure deployment to the latest version. Required for update
bool     PrivateNetworking = false; | Y | Y | N | Available starting version 2.2. Set to true to create the host VM without public IP address. If set, VnetResourceGroupName, VnetName and SubnetName must be provided (and already exist). The deployment must be initiated from a machine that has access to that subnet.
bool     KeepSshPortOpen =   false; | Y | Y | Y | Available starting version 3.0. Set to true if you need to keep the SSH port accessible on the host VM while deployer is not running (not recommended). 
string   LogAnalyticsArmId | Y | N | N | Arm resource id for an exising Log Analytics workspace, workspace is used for App Insights - Not required, a workspace will be generated automatically if not provided.
bool     ProvisionPostgreSqlOnAzure =   false; | Y | N | N | Triggers whether to use Docker MySQL or Azure PostgreSQL when provisioning the database. Required for AKS deployment.
bool     UseAks =   false; | Y | N | N | Uses Azure Kubernetes Service rather than a VM to run the CoA system services Cromwell/TES/TriggerService.
string   AksClusterName | Y | Y | N | Cluster name of existing Azure Kubernetes Service cluster to use rather than provisioning a new one.
string   AksCoANamespace = "coa" | Y | N | N | Kubernetes namespace.
bool     ManualHelmDeployment | Y | N | N | For use if user doesn't have direct access to existing AKS cluster.
string   HelmBinaryPath = "C:\\ProgramData\\chocolatey\\bin\\helm.exe" | Y | N | N | Path to helm binary for AKS deployment.
int      AksPoolSize = 2 | Y | N | N | Size of AKS node pool, two nodes are recommended for reliability, however a minimum of one can be used to save COGS. 
bool DebugLogging = false | Y | N | N | Prints all log information.
string PostgreSqlServerName | Y | Y | N | Name of existing postgresql server. 
bool UsePostgreSqlSingleServer = false | Y | N | N | Use Postgresql single server rather than flexi servers, only recommended if you need to use private endpoints. 
string KeyVaultName | Y | Y | N | Name of an existing key vault
bool CrossSubscriptionAKSDeployment | Y | N | N | AKS cluster is in a different subscription than the storage account, so a keyvault and storage key will be used for storage auth for AKS.

The following are more advanced configuration parameters:

Configuration   parameter | Has default | Validated | Used by update | Comment
-- | -- | -- | -- | --
string   VnetAddressSpace = "10.1.0.0/16" | Y | N | N | Total address space for CoA vnet.
string   VmSubnetAddressSpace = "10.1.0.0/24" | Y | N | N | Address space for compute, VM or AKS. 
string   MySqlSubnetAddressSpace  = "10.1.1.0/24" | Y | N | N | Address space for database.
string   KubernetesServiceCidr = "10.1.4.0/22" | Y | N | N | Address space for kubernetes system services, must not overlap with any subnets.
string   KubernetesDnsServiceIP = "10.1.4.10" | Y | N | N | Kubernetes DNS service IP Address.
string   KubernetesDockerBridgeCidr = "172.17.0.1/16" | Y | N | N | Kubernetes dock bridge Cidr.

### Use a specific Cromwell version
#### Before deploying Cromwell on Azure
To choose a specific Cromwell version, you can specify the version as a configuration parameter before deploying Cromwell on Azure. Here is an example:

```
.\deploy-cromwell-on-azure.exe --SubscriptionId <Your subscription ID> --RegionName <Your region> --MainIdentifierPrefix <Your string> --CromwellVersion 53
```

This version will persist through future updates until you set it again or revert to the default behavior by specifying `--CromwellVersion ""`. See note below.

#### After Cromwell on Azure has been deployed
After deployment, you can still change the Cromwell docker image version being used.

**Cromwell on Azure version 2.x**

Run the deployer in update mode and specify the new Cromwell version.
```
.\deploy-cromwell-on-azure.exe --Update true --SubscriptionId <Your subscription ID> --ResourceGroupName <Your RG> --VmPassword <Your VM password> --CromwellVersion 54
```

The new version will persist through future updates until you set it again. 
To revert to the default Cromwell version that is shipped with each deployer version, specify `--CromwellVersion ""`. 
Be aware of compatibility issues if downgrading the version. 
The default version is listed [here](../src/deploy-cromwell-on-azure/scripts/env-03-external-images.txt).

**Cromwell on Azure version 1.x**

[Log on to the host VM](#Connect-to-the-host-VM-that-runs-all-the-docker-containers) using the ssh connection string as described in the instructions. Replace image name with the tag of your choice for the "cromwell" service in the `docker-compose.yml` file.<br/>

```
cd /data/cromwellazure/
sudo nano docker-compose.yml
# Modify the cromwell service image name and save the file
```

For these changes to take effect, be sure to restart your Cromwell on Azure VM through the Azure Portal UI or run `sudo reboot`. or run `sudo reboot`. You can also restart the docker containers.

### Use input data files from an existing Azure storage account that my lab or team is currently using

##### If the VM can be granted 'Contributor' access to the storage account:

1. [Add the VM identity as a Contributor](/README.md/#Connect-to-existing-Azure-resources-I-own-that-are-not-part-of-the-Cromwell-on-Azure-instance-by-default) to the Storage Account via Azure Portal or Azure CLI.<br/>

2. Navigate to the "configuration" container in the default storage account. Replace the values below with your Storage Account and Container names and add the line to the end of the `containers-to-mount` file:
    ```
    /yourstorageaccountname/yourcontainername
    ```
3. Save the changes and restart the VM

##### If the VM cannot be granted Contributor access to the storage account:
This is applicable if the VM and storage account are in different Azure tenants, or if you want to use SAS token anyway for security reasons

1. Add a [SAS URL for your desired container](https://docs.microsoft.com/en-us/azure/storage/common/storage-sas-overview) to the end of the `containers-to-mount` file. The SAS token can be at the account or container level and may be read-only or read-write depending on the usage.
    ```
    https://<yourstorageaccountname>.blob.core.windows.net/<yourcontainername>?<sastoken>
    ```

2. Save the changes and restart the VM

In both cases, the specified containers will be mounted as `/yourstorageaccountname/yourcontainername/` on the Cromwell server. You can then use `/yourstorageaccountname/yourcontainername/path` in the trigger, WDL, CWL, inputs and workflow options files.

### Use a batch account for which I have already requested or received increased cores quota from Azure Support
[Log on to the host VM](#Connect-to-the-host-VM-that-runs-all-the-docker-containers) using the ssh connection string as described in the instructions. 

**Cromwell on Azure version 2.x**

Replace `BatchAccountName` variable in the `env-01-account-names.txt` file with the name of the desired batch account and save your changes.<br/>

```
cd /data/cromwellazure/
sudo nano env-01-account-names.txt
# Modify the BatchAccountName to your Batch Account name and save the file
```

**Cromwell on Azure version 1.x**

Replace `BatchAccountName` environment variable for the "tes" service in the `docker-compose.yml` file with the name of the desired batch account and save your changes.<br/>

```
cd /data/cromwellazure/
sudo nano docker-compose.yml
# Modify the BatchAccountName to your Batch Account name and save the file
```

To allow the host VM to use a batch account, [add the VM identity as a Contributor](../README.md/#Connect-to-existing-Azure-resources-I-own-that-are-not-part-of-the-Cromwell-on-Azure-instance-by-default) to the Azure batch account via Azure Portal or Azure CLI.<br/>
To allow the host VM to read prices and information about types of machines available for the batch account, [add the VM identity as a Billing Reader](../README.md/#Connect-to-existing-Azure-resources-I-own-that-are-not-part-of-the-Cromwell-on-Azure-instance-by-default) to the subscription with the configured Batch Account.

For these changes to take effect, be sure to restart your Cromwell on Azure VM through the Azure Portal UI or run `sudo reboot`. or run `sudo reboot`.

### Use private Docker containers hosted on Azure
Cromwell on Azure supports private Docker images for your WDL tasks hosted on [Azure Container Registry or ACR](https://docs.microsoft.com/en-us/azure/container-registry/).
To allow the host VM to use an ACR, [add the VM identity as a Contributor](../README.md/#Connect-to-existing-Azure-resources-I-own-that-are-not-part-of-the-Cromwell-on-Azure-instance-by-default) to the Container Registry via the Azure Portal or Azure CLI.  Also, set "Admin user" to "Enabled" in the ACR's "Access keys" section in the Azure Portal.<br/>

### Configure my Cromwell on Azure instance to always use dedicated batch VMs to avoid getting preempted
By default, your workflows will run on low priority Azure batch nodes.<br/>

If you prefer to use dedicated Azure Batch nodes for all tasks, do the following:

**Cromwell on Azure version 2.x**

In file `cromwell-application.conf`, in the `configuration` container in the default storage account, in backend section, change `preemptible: true` to `preemptible: false`. Save your changes and restart the VM.<br/>

Note that you can override this setting for each task individually by setting the `preemptible` boolean flag to `true` or `false` in the "runtime" attributes section of your task.

**Cromwell on Azure version 1.x**

[Log on to the host VM](#Connect-to-the-host-VM-that-runs-all-the-docker-containers) using the ssh connection string as described in the instructions.  Change the `UsePreemptibleVmsOnly` environment variable for the "tes" service to "false" in the `docker-compose.yml` file and save your changes.<br/>

```
cd /data/cromwellazure/
sudo nano docker-compose.yml
# Modify UsePreemptibleVmsOnly to false and save the file
```

For these changes to take effect, be sure to restart your Cromwell on Azure VM through the Azure Portal UI or run `sudo reboot`.

### Access the Cromwell REST API directly from Linux host VM
Cromwell is run in server mode on the Linux host VM. After [logging in to the host VM](#Connect-to-the-host-VM-that-runs-all-the-docker-containers), it can be accessed via curl as described below:

***Get all workflows***<br/>

`curl -X GET "http://localhost:8000/api/workflows/v1/query" -H  "accept: application/json"`<br/>

***Get specific workflow's status by id***<br/>
`curl -X GET "http://localhost:8000/api/workflows/v1/{id}/status" -H  "accept: application/json"`<br/>

***Get call-caching difference between two workflow calls***<br/>
`curl -X GET "http://localhost:8000/api/workflows/v1/callcaching/diff?workflowA={workflowId1}&callA={workflowName.callName1}&workflowB={workflowId2}&callB={workflowName.callName2}" -H  "accept: application/json"`<br/>

You can perform other Cromwell API calls following a similar pattern. To see all available API endpoints, see Cromwell's REST API [here](https://cromwell.readthedocs.io/en/stable/api/RESTAPI/)


## Performance and Optimization
### Cost analysis for Cromwell on Azure
To learn more about your Cromwell on Azure Resource Group's cost, navigate to the "Cost Analysis" menu item in the "Cost Management" section of your Azure Resource Group on the Azure Portal. More information [here](https://docs.microsoft.com/en-us/azure/cost-management/quick-acm-cost-analysis).<br/>
![RG cost analysis](screenshots/rgcost.PNG)

You can also use the [Pricing Calculator](https://azure.microsoft.com/en-us/pricing/calculator/) to estimate your monthly cost.

### How Cromwell on Azure selects batch VMs to run tasks in a workflow
VM price data is used to select the cheapest per hour VM for a task's runtime requirements and is also stored in the TES database to allow calculation of total workflow cost.  VM price data is obtained from the [Azure RateCard API](https://docs.microsoft.com/en-us/previous-versions/azure/reference/mt219005(v=azure.100)).  Accessing the Azure RateCard API requires the VM's [Billing Reader](https://docs.microsoft.com/en-us/azure/role-based-access-control/built-in-roles#billing-reader) role to be assigned to your Azure subscription scope.  If you don't have [Owner](https://docs.microsoft.com/en-us/azure/role-based-access-control/built-in-roles#owner), or both [Contributor](https://docs.microsoft.com/en-us/azure/role-based-access-control/built-in-roles#contributor) and [User Access Administrator](https://docs.microsoft.com/en-us/azure/role-based-access-control/built-in-roles#user-access-administrator) roles assigned to your Azure subscription, the deployer will not be able to complete this on your behalf - you will need to contact your Azure subscription administrator(s) to complete this for you.  You will see a warning in the TES logs indicating that default VM prices are being used until this is resolved.

By default, all VM sizes supported by Azure Batch are considered for task execution. 

You can constrain the Azure VM sizes or families considered for task execution by modifying the file "allowed-vm-sizes" in the "configuration" storage container and restarting the host VM. For the full list of VM sizes and their features, see file "supported-vm-sizes" in the same container. Note that over-constraining the allowed VM sizes may result in task failures when no suitable VM is found, as well as higher execution costs. Any errors in the "allowed-vm-sizes" will be surfaced in the same file upon host VM restart.

### Optimize my WDLs
This section is COMING SOON.


## Miscellaneous
### Get container logs to debug issues
The host VM is running multiple Docker containers that enable Cromwell on Azure - mysql, broadinstitute/cromwell, cromwellonazure/tes, cromwellonazure/triggerservice. On rare occasions, you may want to debug and diagnose issues with the Docker containers. After [logging in to the host VM](#Connect-to-the-host-VM-that-runs-all-the-docker-containers), run: 
```
sudo docker ps
```

This command will list the names of all the Docker containers currently running. To get logs for a particular container, run: 
```
sudo docker logs 'containerName'
```

### I am running a large number of workflows and MySQL storage disk is full
To ensure that no data is corrupted for MySQL backed storage for Cromwell, Cromwell on Azure mounts MySQL files on to an Azure Managed Data Disk of size 32G. In case there is a need to increase the size of this data disk, follow instructions [here](https://docs.microsoft.com/en-us/azure/virtual-machines/linux/expand-disks#expand-an-azure-managed-disk).

### Running CWL Workflows on Cromwell on Azure
Running workflows written in the Common Workflow Language(CWL) format is possible with a few modifications to your workflow submission.
For CWL workflows, all CWL resource keywords are supported, plus `preemptible` (not in CWL spec). `preemptible` defaults to true (set in Cromwell configuration file), so use `preemptible` only if setting it to false (run on dedicated machine). TES keywords are also supported in CWL workflows, but we advise users to use the CWL ones.<br/>

   *CWL keywords: (CWL workflows only)* <br/>
    coresMin: number <br/>
    ramMin: size in MB <br/>
    tmpdirMin: size in MB - Cromwell on Azure version 2.0 and above only<br/>
    outdirMin: size in MB - Cromwell on Azure version 2.0 and above only<br/>
    (the final disk size is the sum of tmpDir and outDir values) <br/>

   *TES keywords: (both CWL and WDL workflows)* <br/>
    preemptible: true|false <br/>

**Cromwell on Azure version 1.x known issue for CWL files: Cannot request specific HDD size** Unfortunately, this is actually a [bug in how Cromwell](https://broadworkbench.atlassian.net/jira/software/c/projects/BA/issues/BA-4507) currently parses the CWL files and thus must be addressed in the Cromwell source code directly.
The current workaround for this is to increase the number of `vCPUs` or `memory` requested for a task, which will indirectly increase the amount of working disk space available. However, because this may cause inconsistent performance, we advise that if you are running a task that might consume a large amount of local scratch space, consider converting your workflow to the WDL format instead.





