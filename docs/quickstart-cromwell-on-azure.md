# Quickstart
This quickstart walks through how to install Cromwell on Azure and run a sample workflow. 

Get started in just a few steps: 
1. Installation: Download prerequisites and use the installation executable to configure the Azure resources needed to run Cromwell on Azure. 
2. Create a JSON file with required URLs. 
3. Run the sample workflow.


# Installation: 
## Installation Prerequisites
Before installing Cromwell on Azure, be sure that you have [Azure Command Line Interface (az cli)](https://docs.microsoft.com/en-us/cli/azure/?view=azure-cli-latest), a command line experience for managing Azure resources. Install it from [here](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli?view=azure-cli-latest). Once you have "az cli" installed, run `az login` on PowerShell, command line or terminal and use your Azure credentials to get started!<br/>

You will also need to set up your [Azure Subscription](https://portal.azure.com/) before installing Cromwell on Azure.<br/>

To ensure an error free installation, check your Azure Batch account quotas. Learn more [here](https://docs.microsoft.com/en-us/azure/batch/batch-quota-limit#resource-quotas).<br/>

## Download the installation executable
There are two ways to obtain the installation executable: 
1. Download the required executable from [GitHub releases](https://github.com/microsoft/CromwellOnAzure/releases). Choose the runtime of your choice from win-x64, linux-x64, osx-x64
2. Clone the [Cromwell on Azure repository](https://github.com/microsoft/CromwellOnAzure) and build the solution in Visual Studio 2019. Note that [VS 2019](https://visualstudio.microsoft.com/vs/) and [.NET Core SDK 3.0 and 2.2.x](https://dotnet.microsoft.com/download/dotnet-core) are required prerequisites. Build and [publish](https://docs.microsoft.com/en-us/dotnet/core/tools/dotnet-publish?tabs=netcore21#synopsis) the solution (remember to use the correct runtime identifier) to produce the executable

## Configure and run the installation executable
To run the executable on Linux or OS X, assign execute permissions to the file by running the following command on the terminal: 
`chmod +x <fileName>`<br/>
Replace `fileName` with the correct name: `deploy-cromwell-on-azure-linux` or `deploy-cromwell-on-azure-osx.app`.<br/>

The installation executable needs to be configured and run from the command line. You will need the following parameters:
- *Required*: Your Azure `SubscriptionId`. For more details see [Subscriptions](https://account.azure.com/Subscriptions).
- *Required*: The `RegionName` specifies the region you would like to use for your Cromwell on Azure instance. To find a list of all available regions, run `az account list-locations` on command line or PowerShell and use the desired region's "name" property for `RegionName`.
- *Optional*: `MainIdentifierPrefix`. This string, followed by a GUID will be used to name your Cromwell on Azure resource group and associated resources. If not specified, default value of "coa" followed by a GUID is used as a prefix for the Azure Resource Group and all Azure resources created for your Cromwell on Azure instance. After installation, you can search for your resources using the `MainIdentifierPrefix` value.<br/>

Run the following at command line or terminal after navigating to where your executable is saved:
```
.\deploy-cromwell-on-azure.exe --SubscriptionId <Your subscription ID> --RegionName <Your region> --MainIdentifierPrefix <Your string> 
```

Installation can take up to 20 minutes to complete.

## Cromwell on Azure installed resources
Once installed, Cromwell on Azure configures the following Azure resources:

* [Host VM](https://azure.microsoft.com/en-us/services/virtual-machines/) - The host VM runs the Cromwell server.  It includes the virtual machine, disk, network interface, public IP address, and virtual network. 
* [Batch account](https://docs.microsoft.com/en-us/azure/batch/) - The Batch account is connected to the host VM by default and will spin up the virtual machines that run each task in a workflow. 
* [Storage account](https://docs.microsoft.com/en-us/azure/storage/) - This Storage account is mounted to the host VM. By default, it includes the following Blob containers - "cromwell-executions", "cromwell-workflow-logs", "inputs", "outputs", and "workflows".
* [Application Insights](https://docs.microsoft.com/en-us/azure/azure-monitor/app/app-insights-overview) - This contains all logs from the workflow to enable debugging at the task level. 
* [Cosmos DB](https://docs.microsoft.com/en-us/azure/cosmos-db/introduction) - This database includes information and metadata about each task in each workflow run by the host VM.

 All of these resources will be grouped under a single resource group in your account, which you can view on the [Azure Portal](https://portal.azure.com). Note that your specific Resource Group name, host VM name and host VM password for username "vmadmin" are printed to the screen during installation. We recommend safely storing these for your future use! <br/>

Note that as part of the Cromwell on Azure installation, a "Hello World" workflow is automatically run. The input files for this workflow are found in the "inputs" container, and the output files can be found in the "cromwell-executions" container.<br/>


# Run a sample workflow
To run a workflow using Cromwell on Azure, you will need to upload your input files and your WDL file to Azure storage. You will also need to generate a Cromwell on Azure-specific trigger file which includes the path to your WDL and inputs file, and any workflow options and dependencies. Submitting this trigger file initiates the Cromwell workflow. In this example, we will run a sample workflow written in WDL that converts FASTQ files to uBAM for chromosome 21.

## Access input data 
You can find publicly available paired end reads for chromosome 21 hosted here:

[https://msgenpublicdata.blob.core.windows.net/inputs/chr21/chr21.read1.fq.gz](https://msgenpublicdata.blob.core.windows.net/inputs/chr21/chr21.read1.fq.gz)
[https://msgenpublicdata.blob.core.windows.net/inputs/chr21/chr21.read2.fq.gz](https://msgenpublicdata.blob.core.windows.net/inputs/chr21/chr21.read2.fq.gz)

You can use these input files directly as they are publicly available.<br/>

Alternatively, you can choose to upload the data into the "inputs" container in your Cromwell on Azure storage account associated with your host VM.
You can do this directly from the Azure Portal, or use various tools including [Microsoft Azure Storage Explorer](https://azure.microsoft.com/features/storage-explorer/), [blobporter](https://github.com/Azure/blobporter), or [AzCopy](https://docs.microsoft.com/azure/storage/common/storage-use-azcopy?toc=%2fazure%2fstorage%2fblobs%2ftoc.json). <br/>

## Get inputs JSON and WDL files
You can find an inputs JSON file and a sample WDL for converting FASTQ to uBAM format in this [GitHub repo](https://github.com/microsoft/CromwellOnAzure/blob/master/samples/quickstart). The chr21 FASTQ files are hosted on the public Azure Storage account container.<br/>
You can use the "msgenpublicdata" storage account directly as a relative path, like the below example.<br/>

The inputs JSON file should contain the following:
```
{
  "FastqToUbamSingleSample.sample_name": "chr21",
  "FastqToUbamSingleSample.library_name": "Pond001",
  "FastqToUbamSingleSample.group_name": "GrA",
  "FastqToUbamSingleSample.platform": "illumina",
  "FastqToUbamSingleSample.platform_unit": "GrA.chr21",
  "FastqToUbamSingleSample.fastq_pair": [
    "/msgenpublicdata/inputs/chr21/chr21.read1.fq.gz",
    "/msgenpublicdata/inputs/chr21/chr21.read2.fq.gz"
  ]
}
```

The input path consists of 3 parts - the storage account name, the blob container name, file path with extension. Example file path for an "inputs" container in a storage account "msgenpublicdata" will look like
`"/msgenpublicdata/inputs/chr21/chr21.read1.fq.gz"`

If you chose to host these files on your own Storage account, replace the name "msgenpublicdata/inputs" to your `<storageaccountname>/<containername>`. <br/>
 
Alternatively, you can use http or https paths for your input files [using shared access signatures (SAS)](https://docs.microsoft.com/en-us/azure/storage/common/storage-sas-overview) for files in a private Azure Storage account container or refer to any public file location. 

Please note, [Cromwell engine currently does not support http(s) paths](https://github.com/broadinstitute/cromwell/issues/4184#issuecomment-425981166)in the JSON inputs file that accompany a WDL. So ensure that your workflow WDL does not perform any WDL operations/input expressions that require Cromwell to download the http(s) inputs on the host machine.

## Configure your Cromwell on Azure trigger file
Cromwell on Azure uses a trigger file to note the paths to all input information and to initiate the workflow. A sample trigger file can be downloaded from this [GitHub repo](https://github.com/microsoft/CromwellOnAzure/blob/master/samples/quickstart/FastqToUbamSingleSample.chr21.json) and includes the following information:
- The "WorkflowUrl" is the url for your WDL file. You can get this from the Azure Portal.
- The "WorkflowInputsUrl" is the url for your input json file.
- The "WorkflowOptionsUrl" is only used with some WDL files. If you are not using it set this to `null`.
- The "WorkflowDependenciesUrl" is only used with some WDL files. If you are not using it set this to `null`.

Your trigger file should be configured as follows:
```
{
 "WorkflowUrl": <URL path to your WDL in quotes>,
 "WorkflowInputsUrl": <URL path to your input json in quotes>,
 "WorkflowOptionsUrl": null,
 "WorkflowDependenciesUrl": null
}
```

When using WDL and inputs JSON file hosted on your private Azure Storage account's blob containers, the specific url can be found by clicking on the file to view the blob's properties from the Azure portal. The URL path to "WorkflowUrl" for a test WDL file will look like:
`https://<storageaccountname>.blob.core.windows.net/inputs/test/test.wdl`

Alternatively, you can use any http or https path to a TES compliant WDL and inputs.json [using shared access signatures (SAS)](https://docs.microsoft.com/en-us/azure/storage/common/storage-sas-overview) for files in a private Azure Storage account container or refer to any public file location. 

You can also host your WDL and JSON inputs files on your Storage account container and use the `/<storageaccountname>/<containername>/blobName` format.

## Start a WDL workflow
To start a WDL workflow, go to your Cromwell on Azure Storage account associated with your host VM. In the "workflows" container, create the directory "new" and place the trigger file in that folder. This initiates a Cromwell workflow, and returns a workflow id that is appended to the trigger JSON file name and transferred over to the "inprogress" directory in the Workflows container.<br/>

![directory](/docs/screenshots/newportal.PNG)
![directory2](/docs/screenshots/newexplorer.PNG)

For example, a trigger JSON file with name `task1.json` in the "new" directory, will be move to "inprogress" directory with a modified name `task1.guid.json`. This guid is a workflow id assigned by Cromwell.<br/>

Once your workflow completes, you can view the output files of your workflow in the "cromwell-executions" container within your Azure Storage Account. Additional output files from the cromwell endpoint, including metadata and the timing file, are found in the "outputs" container. To learn more about Cromwell's metadata and timing information, visit the [Cromwell documentation](https://cromwell.readthedocs.io/en/stable/).<br/>
