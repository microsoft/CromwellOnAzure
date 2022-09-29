# Deploying host configurations in Cromwell on Azure
#### Getting started
To be written.

## Host Configurations
All host configurations are directories of the `HostConfigs` directory in the root of the repo.
* Each directory's name consists of a *name*_*version* pair.
* Every configuration has a `config.json` file, along with any other files that cannot be downloaded.
* Configuration script files must be named `start-task.sh`, whether standing alone or included in the zip.
* It's not recommended to include a the script both in the zip and in the directory. In the case where a zip is provided, it is recommended to include the script in the zip.
* If the host configuration can be used in a compute node that has no access to the internet, its recommeded to provide two configurations (one for internet access, the other for completely isolated deployments).

The only files consumed in each host configuration directory are:
* config.json
* start-task.sh
* start.zip

It is recommended to include a README.md, a LICENSE (or similar) file, etc. as appropriate. Note that, if included in the CoA repo, evertything here will be visible to the entire internet. In the future, they may be provided to those determining which configuration to deploy. None of those files will be placed into the deployment itself, however.

### Configuration file syntax
Host configuration files are JSON files. These are the items that can be provided. None are required, but the `config.json` itself is required.
* batchImage
* virtualMachineSizes
* startTask
* dockerRun

#### BatchImage
`BatchImage` identifies the image to deploy onto the compute node(s). Currently, only Azure Virtual Machines Marketplace Images are supported. There are two items, both are required:
* imageReference
* nodeAgentSKUId - the SKU of Batch Node Agent to be provisioned on the compute node.

This is how the OS is determined. If this is not provided, the default image will be used. If the default image has not been personalized, it will default to ubuntu.

##### ImageReference
All four items are required:
* offer
* publisher
* sku
* version

#### VirtualMachineSizes
This is an array of possible `vmsize`-related target VM types available in Azure. Anything provided here will only be used if allowed in the CoA deployment. Each entry contains the following items:
* container - If specified, if this matches the executor image, this configuration will be applied.
* vmSize - If this is specified, `familyName` and `minVmSize` must not be provided. This forces only this vmSize to be used. If this size is not allowed, the tasks will fail.
* familyName - This will use the least expensive (in terms of cost/time) vmSize in the provided family of VmSizes that meets the tasks size requirements.
* minVmSize - Within a VmSize family, this sets the minimum size that could be selected. Note that providing this implies the `familyName`, so there's no need to provide both. Note that, if this size doesn't meat any of the task's other size constraints, a larger size within the same family will be attempted.

#### StartTask
The script itself must be provided as a file. It is recommended to provide the file in the same directory as the configuration file, named `start-task.sh`, or include it in a Batch Application zip file, where the strip itself is named `start-task.sh`. Every file provided here will be downloaded before the start script is called. Note that currently, no authentication is possible (unless embedded in the URL, making it public if this is included in CoA's repo).
If this item is included, it contains the following items:
* resourceFiles

##### ResourceFiles
This is an array of URLs to download. The following items describe the download:
* httpUrl - Required. The URL of the file to download.
* filePath - Required. The location on the Compute Node to which to download the file(s), relative to the task's working directory. Must include the filename, and must be a realative path..
* fileMode - Optional. The file permission mode attribute in octal format. Defaults to 0770 (aka ug+rw).

#### DockerRun
Container invocation related info is provided here.
* parameters - Optional. Additional parameters to be added to the `docker run` that runs each task's command (above and beyond what CoA already provides). Usually used to expose access to drivers, etc.
* pretaskCommand - Optional. Array of arguments for a single command to run inside of the executor image before each task. A shell is not provided, and no expansion of the commandline is performed. No `-c` is indserted between the first and subsequent elements. All except the first element is quoted.

