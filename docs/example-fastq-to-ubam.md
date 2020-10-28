# Example workflow to convert FASTQ files to uBAM file

This document describes how to run an example workflow that converts a pair of FASTQ files to one uBAM file. This may be combined to other WDL (Workflow Description Language) files and used as part of a larger workflow. 

## Run a sample workflow

In this example, we will run a workflow written in WDL that converts a pair of FASTQ files to uBAM for a small part of the NA12878 sample.


## Access input data 

You can find publicly available paired end reads for the sample hosted here:

FASTQ 1<br/>
[https://datasettestinputs.blob.core.windows.net/dataset/seq-format-conversion/NA12878_20k/H06HDADXX130110.1.ATCACGAT.20k_reads_1.fastq](https://datasettestinputs.blob.core.windows.net/dataset/seq-format-conversion/NA12878_20k/H06HDADXX130110.1.ATCACGAT.20k_reads_1.fastq)

FASTQ 2<br/>
[https://datasettestinputs.blob.core.windows.net/dataset/seq-format-conversion/NA12878_20k/H06HDADXX130110.1.ATCACGAT.20k_reads_2.fastq](https://datasettestinputs.blob.core.windows.net/dataset/seq-format-conversion/NA12878_20k/H06HDADXX130110.1.ATCACGAT.20k_reads_2.fastq)

You can use these input file URLs directly as they are publicly available.<br/>

Alternatively, you can choose to upload the data into the "dataset" container in your Cromwell on Azure storage account associated with your host VM.
You can use [AzCopy](https://docs.microsoft.com/en-us/azure/storage/common/storage-use-azcopy-blobs#copy-a-container-to-another-storage-account) to transfer the required files to your own Storage account [using a shared access signature](https://docs.microsoft.com/en-us/azure/storage/common/storage-sas-overview) with "Write" access.<br/>

```[//]: # ([SuppressMessage\("Microsoft.Security", "CS002:SecretInNextLine", Justification="public dataset"\)]) 
.\azcopy.exe copy 'https://datasettestinputs.blob.core.windows.net/dataset/seq-format-conversion?sv=2018-03-28&sr=c&si=coa&sig=nKoK6dxjtk5172JZfDH116N6p3xTs7d%2Bs5EAUE4qqgM%3D' 'https://<destination-storage-account-name>.blob.core.windows.net/dataset?<WriteSAS-token>' --recursive --s2s-preserve-access-tier=false
```
You can also do this directly from the Azure Portal, or use other tools including [Microsoft Azure Storage Explorer](https://azure.microsoft.com/features/storage-explorer/) or [blobporter](https://github.com/Azure/blobporter). <br/>

## Configure your Cromwell on Azure trigger JSON, inputs JSON and WDL files

You can find [an inputs JSON file](https://github.com/microsoft/seq-format-conversion-azure/blob/main-azure/paired-fastq-to-unmapped-bam.inputs.json) and [a WDL script](https://github.com/microsoft/seq-format-conversion-azure/blob/main-azure/paired-fastq-to-unmapped-bam.wdl) for converting FASTQ to uBAM format in [Sequence data format conversion pipelines repo](https://github.com/microsoft/seq-format-conversion-azure). The FASTQ files are hosted on the public Azure Storage account container.<br/>
You can use the "datasettestinputs" storage account directly as a relative path, like the below example.<br/>

The inputs JSON file should contain the following:
```
{
  "ConvertPairedFastQsToUnmappedBamWf.readgroup_name": "NA12878_A",
  "ConvertPairedFastQsToUnmappedBamWf.sample_name": "NA12878",
  "ConvertPairedFastQsToUnmappedBamWf.fastq_1": "/datasettestinputs/dataset/seq-format-conversion/NA12878_20k/H06HDADXX130110.1.ATCACGAT.20k_reads_1.fastq",
  "ConvertPairedFastQsToUnmappedBamWf.fastq_2": "/datasettestinputs/dataset/seq-format-conversion/NA12878_20k/H06HDADXX130110.1.ATCACGAT.20k_reads_2.fastq", 
  "ConvertPairedFastQsToUnmappedBamWf.library_name": "Solexa-NA12878",
  "ConvertPairedFastQsToUnmappedBamWf.platform_unit": "H06HDADXX130110.2.ATCACGAT",
  "ConvertPairedFastQsToUnmappedBamWf.run_date": "2016-09-01T02:00:00+0200",
  "ConvertPairedFastQsToUnmappedBamWf.platform_name": "illumina",
  "ConvertPairedFastQsToUnmappedBamWf.sequencing_center": "BI",
  "ConvertPairedFastQsToUnmappedBamWf.make_fofn": true  
}
```

The input path consists of 3 parts - the storage account name, the blob container name, file path with extension. Example file path for an "dataset" container in a storage account "datasettestinputs" will look like
`"/datasettestinputs/dataset/seq-format-conversion/NA12878_20k/H06HDADXX130110.1.ATCACGAT.20k_reads_1.fastq"`

If you chose to host these files on your own storage account, replace the name "datasettestinputs/dataset" to your `<storageaccountname>/<containername>`. <br/>
 
Alternatively, you can use http or https paths for your input files [using shared access signatures (SAS)](https://docs.microsoft.com/en-us/azure/storage/common/storage-sas-overview) for files in a private Azure Storage account container or refer to any public file location. 

*Please note, [Cromwell engine currently does not support http(s) paths](https://github.com/broadinstitute/cromwell/issues/4184#issuecomment-425981166) in the JSON inputs file that accompany a WDL. **Ensure that your workflow WDL does not perform any WDL operations/input expressions that require Cromwell to download the http(s) inputs on the host machine.***

[A sample trigger JSON file](https://github.com/microsoft/seq-format-conversion-azure/blob/main-azure/paired-fastq-to-unmapped-bam.trigger.json) can be downloaded from [Sequence data format conversion pipelines repo](https://github.com/microsoft/seq-format-conversion-azure) and includes
```
{  
  "WorkflowUrl": "https://raw.githubusercontent.com/microsoft/seq-format-conversion-azure/az3.0.0/paired-fastq-to-unmapped-bam.wdl",
  "WorkflowInputsUrl": "https://raw.githubusercontent.com/microsoft/seq-format-conversion-azure/az3.0.0/paired-fastq-to-unmapped-bam.inputs.json",
  "WorkflowOptionsUrl": null,
  "WorkflowDependenciesUrl": null
}
```
