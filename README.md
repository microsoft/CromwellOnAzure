# Welcome to Cromwell on Azure
![Logo](/docs/screenshots/logo.png)

[Cromwell](https://cromwell.readthedocs.io/en/stable/) is a workflow management system for scientific workflows, orchestrating the computing tasks needed for genomics analysis. Originally developed by the [Broad Institute](https://github.com/broadinstitute/cromwell), Cromwell is also used in the GATK Best Practices genome analysis pipeline. Cromwell supports running scripts at various scales, including your local machine, a local computing cluster, and on the cloud. <br />

Cromwell on Azure configures all Azure resources needed to run workflows with Cromwell on the Microsoft Azure cloud, and uses the [GA4GH TES](https://cromwell.readthedocs.io/en/develop/backends/TES/) backend for orchestrating the tasks that create a workflow. The installation sets up an Azure Kubernetes cluster to run the Cromwell, TES, and Trigger Service containers, and uses the Azure Batch PaaS service to execute each task in a workflow in its own VM, enabling scale-out to thousands of machines. Cromwell workflows can be written using the [WDL](https://github.com/openwdl/wdl) scripting language. To see examples of WDL scripts - see this ['Learn WDL'](https://github.com/openwdl/learn-wdl) repository on GitHub.<br />

### Latest release
 * https://github.com/microsoft/CromwellOnAzure/releases

### Documentation
All documentation has been moved to our [wiki](https://github.com/microsoft/CromwellOnAzure/wiki)!

[Getting Started?](https://github.com/microsoft/CromwellOnAzure/wiki/Getting-Started)

[Got Questions?](https://github.com/microsoft/CromwellOnAzure/wiki/FAQ-And-Troubleshooting)

### Want to Contribute?
Check out our [contributing guidelines](https://github.com/microsoft/CromwellOnAzure/blob/main/docs/contributing.md) and [Code of Conduct](https://github.com/microsoft/CromwellOnAzure/blob/main/CODE_OF_CONDUCT.md) and submit a PR! We'd love to have you.

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
