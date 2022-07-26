# Welcome to Cromwell on Azure
### Latest release
 * [Release 3.1.0](https://github.com/microsoft/CromwellOnAzure/releases/tag/3.1.0)<br/>
 [Release notes for version 3.1.0](docs/release-notes/3.1.0.md)
 
Check the "Update Instructions" section in the version 3.1.0 [release notes](docs/release-notes/3.1.0.md/#update-instructions) to learn how to update an existing Cromwell on Azure deployment to version 3.1.0. You can customize some parameters when updating. Please [see these customization instructions](docs/troubleshooting-guide.md/#Customize-your-Cromwell-on-Azure-deployment), specifically the "Used by update" and "Comment" columns in the table.<br/>

#### Getting started
 * What is [Cromwell on Azure?](#Cromwell-on-Azure) <br/>
 * Deploy Cromwell on Azure now using this [guide](#Deploy-your-instance-of-Cromwell-on-Azure)<br/>
 * A brief [demo video](https://youtu.be/QlRQ63n_mKw) on how to run workflows using Cromwell on Azure<br/>
 * When working with the code, please note that the stable branch is `main`.  The `develop` branch is unstable, and is used for development between releases.

#### Running workflows
 * Prepare, start or abort your workflow [using this guide](docs/managing-your-workflow.md/#Managing-your-workflow)<br/>
 * Here is an example workflow to [convert FASTQ files to uBAM files](docs/example-fastq-to-ubam.md/#Example-workflow-to-convert-FASTQ-files-to-uBAM-files)<br/>
 * Have an existing WDL file that you want to run on Azure? [Modify your existing WDL with these adaptations for Azure](docs/change-existing-WDL-for-Azure.md/#How-to-modify-an-existing-WDL-file-to-run-on-Cromwell-on-Azure)<br/>
 * Want to run commonly used workflows? [Find links to ready-to-use workflows here](#Run-Common-Workflows)<br/>
 * Want to see some examples of tertiary analysis or other genomics analysis? [Find links to related project here](#Related-Projects)<br/>

#### Questions?
 * See our [Troubleshooting Guide](docs/troubleshooting-guide.md/#FAQs,-advanced-troubleshooting-and-known-issues-for-Cromwell-on-Azure) for more information<br/>
 * Known issues and work-arounds are [documented here](docs/troubleshooting-guide.md/#Known-Issues-And-Mitigation)<br/>

If you are running into an issue and cannot find any information in the troubleshooting guide, please open a GitHub issue!<br/>

![Logo](/docs/screenshots/logo.png)

## Cromwell on Azure 

[Cromwell](https://cromwell.readthedocs.io/en/stable/) is a workflow management system for scientific workflows, orchestrating the computing tasks needed for genomics analysis. Originally developed by the [Broad Institute](https://github.com/broadinstitute/cromwell), Cromwell is also used in the GATK Best Practices genome analysis pipeline. Cromwell supports running scripts at various scales, including your local machine, a local computing cluster, and on the cloud. <br />

Cromwell on Azure configures all Azure resources needed to run workflows through Cromwell on the Azure cloud, and uses the [GA4GH TES](https://cromwell.readthedocs.io/en/develop/backends/TES/) backend for orchestrating the tasks that create a workflow. The installation sets up a VM host to run the Cromwell server and uses Azure Batch to spin up virtual machines that run each task in a workflow. Cromwell workflows can be written using either the [WDL](https://github.com/openwdl/wdl) or the [CWL](https://www.commonwl.org/) scripting languages. To see examples of WDL scripts - see this ['Learn WDL'](https://github.com/openwdl/learn-wdl) repository on GitHub. To see examples of CWL scripts - see this ['CWL search result'](https://dockstore.org/search?descriptorType=CWL&searchMode=files) on Dockstore.<br />

## Deploy your instance of Cromwell on Azure

### Prerequisites

1. You will need an [Azure Subscription](https://portal.azure.com/) to deploy Cromwell on Azure.
2. You must have the proper [Azure role assignments](https://docs.microsoft.com/en-us/azure/role-based-access-control/overview) to deploy Cromwell on Azure.  To check your current role assignments, please follow [these instructions](https://docs.microsoft.com/en-us/azure/role-based-access-control/check-access).  You must have one of the following combinations of [role assignments](https://docs.microsoft.com/en-us/azure/role-based-access-control/built-in-roles):
   1. `Owner` of the subscription<br/>
   2. `Contributor` and `User Access Administrator` of the subscription
   3. `Owner` of the resource group. *Note: this level of access will result in a warning during deployment, and will not use the latest VM pricing data.</i>  [Learn more](/docs/troubleshooting-guide.md/#How-are-Batch-VMs-selected-to-run-tasks-in-a-workflow?).  Also, you must specify the resource group name during deployment with this level of access (see below).*
   4.  Note: if you only have `Service Administrator` as a role assignment, please assign yourself as `Owner` of the subscription.
3. Install the [Azure Command Line Interface (az cli)](https://docs.microsoft.com/en-us/cli/azure/?view=azure-cli-latest), a command line experience for managing Azure resources.
4. Run `az login` to authenticate with Azure.


### Download the deployment executable

Download the required executable from [Releases](https://github.com/microsoft/CromwellOnAzure/releases). Choose the runtime of your choice from `win-x64`, `linux-x64`, `osx-x64`. *On Windows machines, we recommend using the `win-x64` runtime (deployment using the `linux-x64` runtime via the Windows Subsystem for Linux is not supported).*<br/>

### Optional: build the executable yourself
Note: Build instructions only provided for the latest release.

#### Linux
*Preqrequisites*:<br/>
.NET 6 SDK for [Linux](https://docs.microsoft.com/en-us/dotnet/core/install/linux). Get instructions for your Linux distro and version to install the SDK. 

For example, instructions for *Ubuntu 18.04* are available [here](https://docs.microsoft.com/en-us/dotnet/core/install/linux-ubuntu#1804-) and below for convenience:

```
wget https://packages.microsoft.com/config/ubuntu/18.04/packages-microsoft-prod.deb -O packages-microsoft-prod.deb
sudo dpkg -i packages-microsoft-prod.deb
rm packages-microsoft-prod.deb
sudo apt-get update && \
sudo apt-get install -y apt-transport-https && \
sudo apt-get update && \
sudo apt-get install -y dotnet-sdk-6.0
```

#### Windows
*Preqrequisites*:<br/>
.NET 6 SDK for [Windows](https://dotnet.microsoft.com/download). Get the executable and follow the wizard to install the SDK.

*Recommended*:<br/>
VS 2022

#### Build steps
1. Clone the [Cromwell on Azure repository](https://github.com/microsoft/CromwellOnAzure)
2. Build the solution using `dotnet build` on bash or Powershell. For Windows, you can choose to build and test using VS 2022
3. Run tests using `dotnet test` on bash or Powershell
4. [Publish](https://docs.microsoft.com/en-us/dotnet/core/tools/dotnet-publish#synopsis) the `deploy-cromwell-on-azure` project [as a self-contained deployment with your target runtime identifier (RID)](https://docs.microsoft.com/en-us/dotnet/core/deploying/#self-contained-deployments-scd) to produce the executable

Example<br/> 
Linux: `dotnet publish -r linux-x64`<br/>
Windows: `dotnet publish -r win-x64`<br/>

Learn more about `dotnet` commands [here](https://docs.microsoft.com/en-us/dotnet/core/tools/dotnet#dotnet-commands)

### Run the deployment executable

1. **Linux and OS X only**: assign execute permissions to the file by running the following command on the terminal:<br/>
`chmod +x <fileName>`. Replace `<fileName>` with the correct name: `deploy-cromwell-on-azure-linux` or `deploy-cromwell-on-azure-osx.app`
1. You must specify the following parameters:
   1. `SubscriptionId` (**required**)
      1.  This can be obtained by navigating to the [subscriptions blade in the Azure portal](https://portal.azure.com/#blade/Microsoft_Azure_Billing/SubscriptionsBlade)
   1. `RegionName` (**required**)
      1. Specifies the region you would like to use for your Cromwell on Azure instance. To find a list of all available regions, run `az account list-locations` on the command line or in PowerShell and use the desired region's "name" property for `RegionName`.
   1. `MainIdentifierPrefix` (*optional*)
      1. This string will be used to prefix the name of your Cromwell on Azure resource group and associated resources. If not specified, the default value of "coa" followed by random characters is used as a prefix for the resource group and all Azure resources created for your Cromwell on Azure instance. After installation, you can search for your resources using the `MainIdentifierPrefix` value.<br/>
   1. `ResourceGroupName` (*optional*, **required** when you only have owner-level access of the *resource group*)
      1. Specifies the name of a pre-existing resource group that you wish to deploy into.
      
Run the following at the command line or terminal after navigating to where your executable is saved:
```
.\deploy-cromwell-on-azure.exe --SubscriptionId <Your subscription ID> --RegionName <Your region> --MainIdentifierPrefix <Your string> 
```

**Example:**
```
.\deploy-cromwell-on-azure.exe --SubscriptionId 00000000-0000-0000-0000-000000000000 --RegionName westus2 --MainIdentifierPrefix coa 
```

A [test workflow](#Hello-World-WDL-test) is run to ensure successful deployment. If your [Batch account does not have enough resource quotas](https://docs.microsoft.com/en-us/azure/batch/batch-quota-limit#resource-quotas), you will see the error while deploying. You can request more quotas by following [these instructions](https://docs.microsoft.com/en-us/azure/batch/batch-quota-limit#increase-a-quota).

Deployment, including a small test workflow can take up to 25 minutes to complete. **At installation, a user is created to allow managing the host VM with username "vmadmin". The password is randomly generated and shown during installation. You may want to save the username, password and resource group name to allow for advanced debugging later.**

Prepare, start or abort a workflow using instructions [here](docs/managing-your-workflow.md).

### Cromwell on Azure deployed resources

Once deployed, Cromwell on Azure configures the following Azure resources:

* [Host VM](https://azure.microsoft.com/en-us/services/virtual-machines/) - runs [Ubuntu 18.04 LTS](https://github.com/microsoft/CromwellOnAzure/blob/421ccd163bfd53807413ed696c0dab31fb2478aa/src/deploy-cromwell-on-azure/Configuration.cs#L16) and [Docker Compose with four containers](https://github.com/microsoft/CromwellOnAzure/blob/master/src/deploy-cromwell-on-azure/scripts/docker-compose.yml) (Cromwell, MySQL, TES, TriggerService).  [Blobfuse](https://github.com/Azure/azure-storage-fuse) is used to mount the default storage account as a local file system available to the four containers.  Also created are an OS and data disk, network interface, public IP address, virtual network, and network security group. [Learn more](https://docs.microsoft.com/en-us/azure/virtual-machines/linux/)
* [Batch account](https://docs.microsoft.com/en-us/azure/batch/) - The Azure Batch account is used by TES to spin up the virtual machines that run each task in a workflow.  After deployment, create an Azure support request to increase your core quotas if you plan on running large workflows.  [Learn more](https://docs.microsoft.com/en-us/azure/batch/batch-quota-limit#resource-quotas)
* [Storage account](https://docs.microsoft.com/en-us/azure/storage/) - The Azure Storage account is mounted to the host VM using [blobfuse](https://github.com/Azure/azure-storage-fuse), which enables [Azure Block Blobs](https://docs.microsoft.com/en-us/rest/api/storageservices/understanding-block-blobs--append-blobs--and-page-blobs) to be mounted as a local file system available to the four containers running in Docker. By default, it includes the following Blob containers - `configuration`, `cromwell-executions`, `cromwell-workflow-logs`, `inputs`, `outputs`, and `workflows`.
* [Application Insights](https://docs.microsoft.com/en-us/azure/azure-monitor/app/app-insights-overview) - This contains logs from TES and the Trigger Service to enable debugging.
* [Cosmos DB](https://docs.microsoft.com/en-us/azure/cosmos-db/introduction) - This database is used by TES, and includes information and metadata about each TES task that is run as part of a workflow.

![Cromwell-On-Azure](/docs/screenshots/cromwellonazure.png)

All of these resources will be grouped under a single resource group in your account, which you can view on the [Azure Portal](https://portal.azure.com). **Note that your specific resource group name, host VM name and host VM password for username "vmadmin" are printed to the screen during deployment. You can store these for your future use, or you can reset the VM's password at a later date via the Azure Portal.**<br/>

You can [follow these steps](/docs/troubleshooting-guide.md/#Use-input-data-files-from-an-existing-Storage-account-that-my-lab-or-team-is-currently-using) if you wish to mount a different Azure Storage account that you manage or own, to your Cromwell on Azure instance.

### Connect to existing Azure resources I own that are not part of the Cromwell on Azure instance by default

Cromwell on Azure uses [managed identities](https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/overview) to allow the host VM to connect to Azure resources in a simple and secure manner.  

At the time of installation, a managed identity is created and associated with the host VM. 

**Cromwell on Azure version 2.x**

Since version 2.0, a user managed identity is created with the name `{resource-group-name}-identity` in the deployment resource group.

**Cromwell on Azure version 1.x**

For version 1.x and below, a system managed identity is created. You can find the identity via the Azure Portal by searching for the VM name in Azure Active Directory, under "All Applications". Or you may use Azure CLI `show` command as described [here](https://docs.microsoft.com/en-us/cli/azure/vm/identity?view=azure-cli-latest#az-vm-identity-show).

To allow the host VM to connect to **custom** Azure resources like Storage Account, Batch Account etc. you can use the [Azure Portal](https://docs.microsoft.com/en-us/azure/role-based-access-control/role-assignments-portal) or [Azure CLI](https://docs.microsoft.com/en-us/azure/role-based-access-control/role-assignments-cli) to find the managed identity of the host VM (if using Cromwell on Azure version 1.x) or the user-managed identity (if using Cromwell on Azure version 2.x and above) and add it as a Contributor to the required Azure resource.<br/>

![Add Role](/docs/screenshots/add-role.png)


For convenience, some configuration files are hosted on your Cromwell on Azure Storage account, in the "configuration" container - `containers-to-mount`, and `cromwell-application.conf`. You can modify and save these file using Azure Portal UI "Edit Blob" option or simply upload a new file to replace the existing one.

![Edit Configuration](/docs/screenshots/edit-config.png)


See [this section in the advanced configuration on details of how to connect a different storage account, batch account, or a private Azure Container Registry](/docs/troubleshooting-guide.md/#Customizing-your-Cromwell-on-Azure-instance).<br/>


For these changes to take effect, be sure to restart your Cromwell on Azure VM through the Azure Portal UI or run `sudo reboot`.

![Restart VM](/docs/screenshots/restartVM.png)


### Hello World WDL test

As part of the Cromwell on Azure deployment, a "Hello World" workflow is automatically run as a check. The input files for this workflow are found in the `inputs` container, and the output files can be found in the `cromwell-executions` container of your default storage account. 
Once it runs to completion you can find the trigger JSON file that started the workflow in the `workflows` container in the `succeeded` directory, if it ran successfully.<br/>

Hello World WDL file:
```
task hello {
  String name

  command {
    echo 'Hello ${name}!'
  }
  output {
	File response = stdout()
  }
  runtime {
	docker: 'ubuntu:16.04'
  }
}

workflow test {
  call hello
}
```

Hello World inputs.json file:
```
{
  "test.hello.name": "World"
}
```

Hello World trigger JSON file as seen in your storage account's `workflows` container in the `succeeded` directory:
```
{
  "WorkflowUrl": "/<storageaccountname>/inputs/test/test.wdl",
  "WorkflowInputsUrl": "/<storageaccountname>/inputs/test/test.json",
  "WorkflowOptionsUrl": null,
  "WorkflowDependenciesUrl": null
}
```

If your "Hello-World" test workflow or other workflows consistently fail, make sure to [check your Azure Batch account quotas](docs/troubleshooting-guide.md/#Check-Azure-Batch-account-quotas).

## Run Common Workflows

Run Broad Institute of MIT and Harvard's Best Practices Pipelines on Cromwell on Azure:

[Data pre-processing for variant discovery](https://github.com/microsoft/gatk4-data-processing-azure)<br/>

[Germline short variant discovery (SNPs + Indels)](https://github.com/microsoft/gatk4-genome-processing-pipeline-azure)<br/>

[Somatic short variant discovery (SNVs + Indels)](https://github.com/microsoft/gatk4-somatic-snvs-indels-azure)<br/>

[RNAseq short variant discovery (SNPs + Indels)](https://github.com/microsoft/gatk4-rnaseq-germline-snps-indels-azure)<br/>

[Variant-filtering with Convolutional Neural Networks](https://github.com/microsoft/gatk4-cnn-variant-filter-azure)<br/>

[Sequence data format conversion](https://github.com/microsoft/seq-format-conversion-azure)<br/>

Other WDL examples on Cromwell on Azure:

[Cromwell Output Re-Organization](https://github.com/microsoft/cromwell-output-reorganization)<br/>

## Related Projects

[Genomics Data Analysis with Jupyter Notebooks on Azure](https://github.com/microsoft/genomicsnotebook)<br/>
