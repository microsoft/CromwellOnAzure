# Managing your workflow
[Prepare](#Prepare-your-workflow) your workflow <br/>
[Start](#Start-your-workflow) your workflow <br/>
[Get your workflow Id](#Get-the-Cromwell-workflow-ID) for your workflow <br/>
[Abort](#Abort-your-workflow) an in-progress workflow <br/>

## Prepare your workflow

### How to prepare a Workflow Description Language (WDL) file that runs a workflow on Cromwell on Azure

For any pipeline, you can create a [WDL](https://software.broadinstitute.org/wdl/) file that calls your tools in Docker containers. Please note that Cromwell on Azure only supports tasks with Docker containers defined for security reasons.<br/>

In order to run a WDL file, you must modify/create a workflow with the following runtime attributes for the tasks that are compliant with the [TES or Task Execution Schemas](https://cromwell.readthedocs.io/en/develop/backends/TES/):

```
runtime {
    cpu: 1
    memory: 2 GB
    disk: 10 GB
    docker:
    maxRetries: 0
    preemptible: true
}
```
Ensure that the attributes `memory` and `disk` (note: use the singular form for `disk` NOT `disks`) have units. Supported units from Cromwell:

> KB - "KB", "K", "KiB", "Ki"<br/>
> MB - "MB", "M", "MiB", "Mi"<br/>
> GB - "GB", "G", "GiB", "Gi"<br/>
> TB - "TB", "T", "TiB", "Ti"<br/>

The `preemptible` attribute is a boolean (not an integer). You can specify `preemptible` as `true` or `false` for each task. When set to `true` Cromwell on Azure will use a [low-priority batch VM](https://docs.microsoft.com/en-us/azure/batch/batch-low-pri-vms#use-cases-for-low-priority-vms) to run the task.<br/>

`bootDiskSizeGb` and `zones` attributes are not supported by the TES backend.<br/>
Each of these runtime attributes are specific to your workflow and tasks within those workflows. The default values for resource requirements are as set above.<br/>
Learn more about Cromwell's runtime attributes [here](https://cromwell.readthedocs.io/en/develop/RuntimeAttributes).

### How to prepare an inputs JSON file to use in your workflow

For specifying inputs to any workflow, you may want to use a JSON file that allows you to customize inputs to any workflow WDL file.<br/>

For files hosted on an Azure Storage account that is connected to your Cromwell on Azure instance, the input path consists of 3 parts - the storage account name, the blob container name, file path with extension, following this format:
```
/<storageaccountname>/<containername>/<blobName>
```

Example file path for an "inputs" container in a storage account "msgenpublicdata" will look like
`"/msgenpublicdata/inputs/chr21.read1.fq.gz"`

### Configure your Cromwell on Azure trigger JSON file

To run a workflow using Cromwell on Azure, you will need to specify the location of your WDL file and inputs JSON file in an Cromwell on Azure-specific trigger JSON file which also includes any workflow options and dependencies. Submitting this trigger file initiates the Cromwell workflow.

All trigger JSON files include the following information:
- The "WorkflowUrl" is the url for your WDL file.
- The "WorkflowInputsUrl" is the url for your input JSON file.
- The "WorkflowOptionsUrl" is only used with some WDL files. If you are not using it set this to `null`.
- The "WorkflowDependenciesUrl" is only used with some WDL files. If you are not using it set this to `null`.

Your trigger file should be configured as follows:
```
{
 "WorkflowUrl": <URL path to your WDL file in quotes>,
 "WorkflowInputsUrl": <URL path to your input json file in quotes>,
 "WorkflowOptionsUrl": <URL path to your workflow options json in quotes>,
 "WorkflowDependenciesUrl": <URL path to your workflow dependencies file in quotes>
}
```

#### Ensure WDL and inputs JSON files are accessible by Cromwell

When using WDL "WorkflowUrl" and inputs JSON "WorkflowInputsUrl" file hosted on your private Azure Storage account's blob containers, the specific URL can be found by clicking on the file to view the blob's properties from the Azure portal. The URL path to "WorkflowUrl" for a test WDL file will look like:
```
https://<storageaccountname>.blob.core.windows.net/inputs/test/test.wdl
```

You can also use the `/<storageaccountname>/<containername>/<blobname>` format for any storage account that is mounted to your Cromwell on Azure instance. By default, Cromwell on Azure mounts a storage account to your instance, which is found in your resource group after a successful deployment. You can [follow these steps](/docs/troubleshooting-guide.md/#Use-input-data-files-from-an-existing-Storage-account-that-my-lab-or-team-is-currently-using) to mount a different storage account that you manage or own, to your Cromwell on Azure instance.

Alternatively, you can use any http or https path to a TES compliant WDL and inputs.json [using shared access signatures (SAS)](https://docs.microsoft.com/en-us/azure/storage/common/storage-sas-overview) for files in a private Azure Storage account container or refer to any public file location like raw GitHub URLs.

#### Ensure your dependencies are accessible by Cromwell

Any additional scripts or subworkflows must be accessible to TES. They can be provided in 3 ways using the "WorkflowDependenciesUrl" property:  
  * Via a ZIP file in a storage container accessible by Cromwell
  * Via http or https URLs (if not public, use [SAS tokens](https://docs.microsoft.com/en-us/azure/storage/common/storage-sas-overview))
  * Via storage accounts accessible to TES using either the `/storageaccountname/containername/blobname notation` OR the `https://<storageaccountname>.blob.core.windows.net/<containername>/<blobname>` format

## Start your workflow

To start a WDL workflow, go to your Cromwell on Azure Storage account associated with your host VM. In the `workflows` container, place the trigger JSON file in the "new" virtual directory (note: virtual directories do not exist on their own, they are just part of a blob's name). This initiates a Cromwell workflow, and returns a workflow ID that is appended to the trigger JSON file name and transferred to the "inprogress" directory in the `workflows` container.<br/>

This can be done programmatically using the [Azure Storage SDKs](https://azure.microsoft.com/en-us/downloads/), or manually via the [Azure Portal](https://portal.azure.com) or [Azure Storage Explorer](https://azure.microsoft.com/en-us/features/storage-explorer/).

### Via the Azure Portal
![Select a blob to upload from the portal](screenshots/newportal.PNG)<br/>

### Via Azure Storage Explorer
![Select a blob to upload from Azure Storage Explorer](screenshots/newexplorer.PNG)

For example, a trigger JSON file with name `task1.json` in the "new" directory, will be move to the "inprogress" directory with a modified name `task1.uuid.json`. This uuid is a workflow ID assigned by Cromwell.<br/>

Once your workflow completes, you can view the output files of your workflow in the `cromwell-executions` container within your Azure Storage Account. Additional output files from the Cromwell endpoint, including metadata and the timing file, are found in the `outputs` container. To learn more about Cromwell's metadata and timing information, visit the [Cromwell documentation](https://cromwell.readthedocs.io/en/stable/).<br/>

## Get the Cromwell workflow ID

The Cromwell workflow ID is generated by Cromwell once the workflow is in progress, and it is appended to the trigger JSON file name.<br/>

For example, placing a trigger JSON file with name `task1.json` in the "new" directory will initiate the workflow.  Once the workflow begins, the JSON file will be moved to the "inprogress" directory in the "workflows" container with a modified name `task1.guid.json`


## Abort your workflow
To abort a workflow that is in-progress, go to your Cromwell on Azure Storage account associated with your host VM. In the `workflows` container, place an empty file in the "abort" virtual directory named `cromwellID.json`, where "cromwellID" is the Cromwell workflow ID you wish to abort.
