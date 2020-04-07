# FAQs, advanced troubleshooting and known issues for Crowmwell on Azure

This article answers FAQs, describes advanced features that allow customization and debugging of Cromwell on Azure, as well as how to diagnose, debug, and work around [known issues](#Known-Issues-And-Mitigation). We are actively tracking these as bugs to be fixed in upcoming releases!

1. Setup
   * I am trying to [setup Cromwell on Azure for multiple users on the same subscription](#Setup-Cromwell-on-Azure-for-multiple-users-in-the-same-Azure-subscription), what are the ways I can do this?
   * I ran the [Cromwell on Azure installer and it failed](#Debug-my-Cromwell-on-Azure-installation-that-ran-into-an-error). How can I fix it?
   * How can I [upgrade my Cromwell on Azure instance?](#Upgrade-my-Cromwell-on-Azure-instance)

2. Analysis
   * I submitted a job and it failed almost immediately. What [happened to it](#Job-failed-immediately)?   
   * How do I [setup my own WDL](#Set-up-my-own-WDL) to run on Cromwell?
   * How can I see [how far along my workflow has progressed?](#Check-all-tasks-running-for-a-workflow-using-Batch-account)
   * My workflow failed at task X. Where should I look to determine why it failed?
     * [Which tasks failed?](#Find-which-tasks-failed-in-a-workflow)
     * Some tasks are stuck or my workflow is stuck in the "inprogess" directory in the "workflows" container. [Were there Azure infrastructure issues?](#Make-sure-there-are-no-Azure-infrastructure-errors)

3. Customizing your instance
   * How do I [use input data files for my workflows from a different Azure Storage account](#Use-input-data-files-from-an-existing-Storage-account-that-my-lab-or-team-is-currently-using) that my lab or team is currently using?
   * Can I connect a different [Batch account with previously increased quotas](#Use-a-Batch-account-for-which-I-have-already-requested-or-received-increased-cores-quota-from-Azure-Support) to run my workflows?
   * How can I [use private docker containers for my workflows?](#Use-private-docker-containers-hosted-on-Azure)
   * A lot of tasks for my workflows run longer than 24 hours and have been randomly stopped. How can I [run all my tasks on dedicated Batch VMs](#Configure-my-Cromwell-on-Azure-instance-to-use-dedicated-Batch-VMs-to-avoid-getting-preempted)
   * Can I get [direct access to Cromwell's REST API?](#Access-the-Cromwell-REST-API-directly-from-Linux-host-VM)

4. Performance & Optimization
   * How can I figure out how much Cromwell on Azure costs me?
     * How much am I [paying for my Cromwell on Azure instance?](#Cost-analysis-for-Cromwell-on-Azure)
     * How are [Batch VMs selected to run tasks in a workflow?](#How-Cromwell-on-Azure-selects-Batch-VMs-to-run-tasks-in-a-workflow)
   * Do you have guidance on how to [optimize my WDLs](#Optimize-my-WDLs)?

5. Miscellanous
   * I cannot find my issue in this document and [want more information](#Get-container-logs-to-debug-issues) from Cromwell, MySQL, or TES docker container logs.
   * I am running a large amount of workflows and [MySQL storage disk is full](#I-am-running-a-large-amount-of-workflows-and-MySQL-storage-disk-is-full)


## Known Issues And Mitigation
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
The root cause is most likely memory pressure on the host Linux VM because [blobfuse](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-how-to-mount-container-linux#overview) processes grow to consume all physical memory. 

You may see the following Cromwell container logs as a symptom:
> Cromwell shutting down because it cannot access the database):
Shutting down cromid-5bd1d24 as at least 15 minutes of heartbeat write errors have occurred between 2020-02-18T22:03:01.110Z and 2020-02-18T22:19:01.111Z (16.000016666666667 minutes

OR
> Failed to instantiate Cromwell System. Shutting down Cromwell.
liquibase.exception.LockException: Could not acquire change log lock.  Currently locked by 012ec19c3285 (172.18.0.4) since 2/19/20 4:10 PM

To mitigate, please resize your VM in the resource group to a machine with at least 14GB memory/RAM. Any workflows still in progress will not be affected.

![Resize VM](/docs/screenshots/resize-VM.png)


## Setup
### Setup Cromwell on Azure for multiple users in the same Azure subscription
This section is COMING SOON.

### Debug my Cromwell on Azure installation that ran into an error
When the Cromwell on Azure installer is run, if there are errors, the logs are printed in the terminal. Most errors are related to insufficient permissions to create resources in Azure on your behalf, or intermittent Azure failures. In case of an error, we terminate the installation process and begin deleting all the resources in the Resource Group if already created. <br/>

Deleting all the resources in the Resource Group may take a while but as soon as you see logs that the Batch account was deleted, you may exit the current process using Ctrl+C or Command+C on terminal/command prompt/PowerShell. The deletion of other Azure resources can continue in the background on Azure. Re-run the installer after fixing any user errors like permissions from the previous try.

If you see an issue that is unrelated to your permissions, and re-trying the installer does not fix it, please file a bug on our GitHub issues.

### Upgrade my Cromwell on Azure instance
Starting in version 1.x, for convenience, some configuration files are hosted on your Cromwell on Azure Storage account, in the "configuration" container - `containers-to-mount`, and `cromwell-application.conf`. You can modify and save these file using Azure Portal UI "Edit Blob" option or simply upload a new file to replace the existing one. Please create the "configuration" container in the if it isn't there already.<br/>

[Follow these steps](https://github.com/microsoft/CromwellOnAzure/releases/tag/1.3.0) to upgrade your Cromwell on Azure instance.


## Analysis
### Job failed immediately
If a workflow you start has a task that failed immediately and lead to workflow failure, be sure to check your input JSON files. Follow the instructions [here](managing-your-workflow.md/#How-to-prepare-an-inputs-JSON-file-to-use-in-your-workflow) and check out an example WDL and inputs JSON file [here](example-fastq-to-ubam.md/#Configure-your-Cromwell-on-Azure-trigger-JSON,-inputs-JSON-and-WDL-files) to ensure there are no errors in defining your input files.

> For files hosted on an Azure Storage account that is connected to your Cromwell on Azure instance, the input path consists of 3 parts - the storage account name, the blob container name, file path with extension, following this format:
```
/<storageaccountname>/<containername>/<blobName>
```

> Example file path for an "inputs" container in a storage account "msgenpublicdata" will look like
`"/msgenpublicdata/inputs/chr21.read1.fq.gz"`

Another possibility is that you are trying to use a Storage account that hasn't been mounted to your Cromwell on Azure instance - either by [default during setup](../README.md/#Cromwell-on-Azure-deployed-resources) or by following these steps to [mount a different Storage account](#Use-input-data-files-from-an-existing-Storage-account-that-my-lab-or-team-is-currently-using). <br/>

Check out these [known issues and mitigation](#Known-Issues-And-Mitigation) for more commonly seen issues caused by bugs we are actively tracking.

### Set up my own WDL
To get started you can view this [Hello World sample](../README.md/#Hello-World-WDL-test), an [example WDL](example-fastq-to-ubam.md) to convert FASTQ to UBAM or [follow these steps](change-existing-WDL-for-Azure.md) to convert an existing public WDL for other clouds to run on Azure. <br/>
There are also links to ready-to-try WDLs for common workflows [here](../README.md/#Run-Common-Omics-Workflows)

**Instructions to write a WDL file for a pipeline from scratch are COMING SOON.**

### Check all tasks running for a workflow using Batch account
Each task in a workflow starts an Azure Batch VM. To see currently active tasks, navigate to your Azure Batch account connected to Cromwell on Azure on Azure Portal. Click on "Jobs" and then search for the Cromwell `workflowId` to see all tasks associated with a workflow.<br/>

![Batch account](screenshots/batch-account.png)

### Find which tasks failed in a workflow
[Cosmos DB](https://azure.microsoft.com/en-us/services/cosmos-db/) stores information about all tasks in a workflow. For monitoring or debugging any workflow you may choose to query the database.<br/>

Navigate to your Cosmos DB instance on Azure Portal. Click on the "Data Explorer" menu item, Click on the "TES" container and select "Items". <br/>

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
When working with Cromwell on Azure, you may run into issues with Azure Batch or Storage accounts. For instance, if a file path cannot be found or if the WDL workflow failed with an unknown reason. For these scenarios, consider debugging or collecting more information using [Application Insights](https://docs.microsoft.com/en-us/azure/azure-monitor/app/app-insights-overview).<br/>

Navigate to your Application Insights instance on Azure Portal. Click on the "Logs (Analytics)" menu item under the "Monitoring" section to get all logs from Cromwell on Azure's TES backend.<br/>

![App insights](screenshots/appinsights.PNG)

You can explore exceptions or logs to find the reason for failure, and use time ranges or [Kusto Query Language](https://docs.microsoft.com/en-us/azure/kusto/query/) to narrow your search.<br/>


## Customizing your Cromwell on Azure instance
### Connect to the host VM that runs all the docker containers

To get logs from all the docker containers or to use the Cromwell REST API endpoints, you may want to connect to the Linux host VM. At installation, a user is created to allow managing the host VM with username "vmadmin". The password is randomly generated and shown during installation. If you need to reset your VM password, you can do this using the Azure Portal or by following these [instructions](https://docs.microsoft.com/en-us/azure/virtual-machines/troubleshooting/reset-password). 

![Reset password](/docs/screenshots/resetpassword.PNG)

To connect to your host VM, you can either
1. Construct your ssh connection string if you have the VM name `ssh vmadmin@<hostname>` OR
2. Navigate to the Connect button on the Overview blade of your Azure VM instance, then copy the ssh connection string. 

Paste the ssh connectiong string in a command line, PowerShell or terminal application to log in.

![Connect with SSH](/docs/screenshots/connectssh.PNG)

### Use input data files from an existing Storage account that my lab or team is currently using
Navigate to the "configuration" container in the Cromwell on Azure Storage account. Replace YOURSTORAGEACCOUNTNAME with your storage account name and YOURCONTAINERNAME with your container name in the `containers-to-mount` file below:
```
/YOURSTORAGEACCOUNTNAME/YOURCONTAINERNAME/
```
Add this to the end of file and save your changes.<br/>

To allow the host VM to write to a Storage account, [add the VM identity as a Contributor](/README.md/#Connect-to-existing-Azure-resources-I-own-that-are-not-part-of-the-Cromwell-on-Azure-instance-by-default) to the Storage Account via Azure Portal or Azure CLI.<br/>

Alternatively, you can choose to add a [SAS url for your desired container](https://docs.microsoft.com/en-us/azure/storage/common/storage-sas-overview) to the end of the `containers-to-mount` file. This is also applicable if your VM cannot be granted Contributor access to the Storage account because the two resources are in different Azure tenants
```
https://<yourstorageaccountname>.blob.core.windows.net:443/<yourcontainername>?<sastoken>
```

When using the newly mounted storage account in your inputs JSON file, use the path `"/container-mountpath/blobName"`, where `container-mountpath` is `/YOURSTORAGEACCOUNTNAME/YOURCONTAINERNAME/`.

For these changes to take effect, be sure to restart your Cromwell on Azure VM through the Azure Portal UI.

### Use a Batch account for which I have already requested or received increased cores quota from Azure Support
[Log on to the host VM](#Connect-to-the-host-VM-that-runs-all-the-docker-containers) using the ssh connection string as described in the instructions. Replace `BatchAccountName` environment variable for the "tes" service in the `docker-compose.yml` file with the name of the desired Batch account and save your changes.<br/>

```
cd /cromwellazure/
sudo nano docker-compose.yml
# Modify the BatchAccountName to your Batch Account name and save the file
```
To allow the host VM to use a batch account, [add the VM identity as a Contributor](../README.md/#Connect-to-existing-Azure-resources-I-own-that-are-not-part-of-the-Cromwell-on-Azure-instance-by-default) to the Azure Batch account via Azure Portal or Azure CLI.<br/>
To allow the host VM to read prices and information about types of machines available for the batch account, [add the VM identity as a Billing Reader](../README.md/#Connect-to-existing-Azure-resources-I-own-that-are-not-part-of-the-Cromwell-on-Azure-instance-by-default) to the subscription with the configured Batch Account.

For these changes to take effect, be sure to restart your Cromwell on Azure VM through the Azure Portal UI or run `sudo reboot`. or run `sudo reboot`.

### Use private docker containers hosted on Azure
Cromwell on Azure supports private docker images for your WDL tasks hosted on [Azure Container Registry or ACR](https://docs.microsoft.com/en-us/azure/container-registry/).
To allow the host VM to use an ACR, [add the VM identity as a Contributor](../README.md/#Connect-to-existing-Azure-resources-I-own-that-are-not-part-of-the-Cromwell-on-Azure-instance-by-default) to the Container Registry via Azure Portal or Azure CLI.<br/>

### Configure my Cromwell on Azure instance to use dedicated Batch VMs to avoid getting preempted
By default, we are using an environment variable `UsePreemptibleVmsOnly` set to true, to always use low priority Azure Batch nodes.<br/>

If you prefer to use dedicated Azure Batch nodes, [log on to the host VM](#Connect-to-the-host-VM-that-runs-all-the-docker-containers) using the ssh connection string as described in the instructions. Replace `UsePreemptibleVmsOnly` environment variable for the "tes" service to "false" in the `docker-compose.yml` file and save your changes.<br/>

```
cd /cromwellazure/
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

### How Cromwell on Azure selects Batch VMs to run tasks in a workflow
VM price data is used to select the most cost-effective VM for a task's runtime requirements, and is also stored in the TES database to allow calculation of total workflow cost.  VM price data is obtained from the [Azure RateCard API](https://docs.microsoft.com/en-us/previous-versions/azure/reference/mt219005(v=azure.100)).  Accessing the Azure RateCard API requires the VM's [Billing Reader](https://docs.microsoft.com/en-us/azure/role-based-access-control/built-in-roles#billing-reader) role to be assigned to your Azure subscription scope.  If you don't have [Owner](https://docs.microsoft.com/en-us/azure/role-based-access-control/built-in-roles#owner), or both [Contributor](https://docs.microsoft.com/en-us/azure/role-based-access-control/built-in-roles#contributor) and [User Access Administrator](https://docs.microsoft.com/en-us/azure/role-based-access-control/built-in-roles#user-access-administrator) roles assigned to your Azure subscription, the deployer will not be able to complete this on your behalf - you will need to contact your Azure subscription administrator(s) to complete this for you.  You will see a warning in the TES logs indicating that default VM prices are being used until this is resolved.

### Optimize my WDLs
This section is COMING SOON.


## Miscellaneous
### Get container logs to debug issues
The host VM is running multiple docker containers that enable Cromwell on Azure - mysql, broadinstitute/cromwell, cromwellonazure/tes, cromwellonazure/triggerservice. On rare occasions, you may want to debug and diagnose issues with the docker containers. After [logging in to the host VM](#Connect-to-the-host-VM-that-runs-all-the-docker-containers), run: 
```
sudo docker ps
```

This command will list the names of all the docker containers currently running. To get logs for a particular container, run: 
```
sudo docker logs 'containerName'
```


### I am running a large amount of workflows and MySQL storage disk is full
To ensure that no data is corrupted for MySQL backed storage for Cromwell, Cromwell on Azure mounts MySQL files on to an Azure Managed Data Disk of size 32G. In case there is a need to increase the size of this data disk, follow instructions [here](https://docs.microsoft.com/en-us/azure/virtual-machines/linux/expand-disks#expand-an-azure-managed-disk).






