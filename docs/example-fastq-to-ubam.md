# Example workflow to convert FASTQ files to uBAM files

This document describes how to run an example workflow that converts input FASTQ files to uBAM files. This may be combined to other WDL (Workflow Description Language) files and used as part of a larger workflow. 

## Run a sample workflow

In this example, we will run a workflow written in WDL that converts FASTQ files to uBAM for chromosome 21.


## Access input data 

You can find publicly available paired end reads for chromosome 21 hosted here:

[https://msgenpublicdata.blob.core.windows.net/inputs/chr21/chr21.read1.fq.gz](https://msgenpublicdata.blob.core.windows.net/inputs/chr21/chr21.read1.fq.gz)
[https://msgenpublicdata.blob.core.windows.net/inputs/chr21/chr21.read2.fq.gz](https://msgenpublicdata.blob.core.windows.net/inputs/chr21/chr21.read2.fq.gz)

You can use these input file URLs directly as they are publicly available.<br/>

Alternatively, you can choose to upload the data into the "inputs" container in your Cromwell on Azure storage account associated with your host VM.
You can do this directly from the Azure Portal, or use various tools including [Microsoft Azure Storage Explorer](https://azure.microsoft.com/features/storage-explorer/), [blobporter](https://github.com/Azure/blobporter), or [AzCopy](https://docs.microsoft.com/azure/storage/common/storage-use-azcopy?toc=%2fazure%2fstorage%2fblobs%2ftoc.json). <br/>


## Configure your Cromwell on Azure trigger JSON, inputs JSON and WDL files

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

If you chose to host these files on your own storage account, replace the name "msgenpublicdata/inputs" to your `<storageaccountname>/<containername>`. <br/>
 
Alternatively, you can use http or https paths for your input files [using shared access signatures (SAS)](https://docs.microsoft.com/en-us/azure/storage/common/storage-sas-overview) for files in a private Azure Storage account container or refer to any public file location. 

*Please note, [Cromwell engine currently does not support http(s) paths](https://github.com/broadinstitute/cromwell/issues/4184#issuecomment-425981166) in the JSON inputs file that accompany a WDL. **Ensure that your workflow WDL does not perform any WDL operations/input expressions that require Cromwell to download the http(s) inputs on the host machine.***

[A sample trigger JSON file can be downloaded from this GitHub repo](https://github.com/microsoft/CromwellOnAzure/blob/master/samples/quickstart/FastqToUbamSingleSample.chr21.json) and includes
```
{
  "WorkflowUrl": "https://raw.githubusercontent.com/microsoft/CromwellOnAzure/master/samples/quickstart/FastqToUbamSingleSample.wdl",
  "WorkflowInputsUrl": "https://raw.githubusercontent.com/microsoft/CromwellOnAzure/master/samples/quickstart/FastqToUbamSingleSample.chr21.inputs.json",
  "WorkflowOptionsUrl": null,
  "WorkflowDependenciesUrl": null
}
```