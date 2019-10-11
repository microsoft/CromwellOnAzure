# Germline alignment and variant calling pipeline
This tutorial walks through how to run the germline alignment and variant calling pipeline, based on [Best Practices Genome Analysis Pipeline by Broad Institute of MIT and Harvard](https://software.broadinstitute.org/gatk/best-practices/workflow?id=11165), on Cromwell on Azure. 

> This WDL pipeline implements data pre-processing and initial variant calling (GVCF generation) according to the GATK Best Practices (June 2016) for germline SNP and Indel discovery in human whole-genome sequencing data.

Learn more [here](https://github.com/microsoft/five-dollar-genome-analysis-pipeline-azure)

### Input data 

All the required input files for the tutorial are on a publicly hosted Azure Storage account

You can find the sample WDL, JSON inputs and the `WholeGenomeGermlineSingleSample.hg38.json` trigger file for the pipeline on the [GitHub repo](https://github.com/microsoft/five-dollar-genome-analysis-pipeline-azure/tree/az1.1.0). 

Trigger JSON file:
```
{
 "WorkflowUrl": "https://raw.githubusercontent.com/microsoft/five-dollar-genome-analysis-pipeline-azure/az1.1.0/WholeGenomeGermlineSingleSample.wdl",
 "WorkflowInputsUrl": "https://raw.githubusercontent.com/microsoft/five-dollar-genome-analysis-pipeline-azure/az1.1.0/WholeGenomeGermlineSingleSample.hg38.inputs.json",
 "WorkflowOptionsUrl": null,
 "WorkflowDependenciesUrl": null
}
```

### Start the workflow
To start the workflow on Cromwell on Azure, place the trigger JSON file in the "new" folder of the "workflows" container within your Cromwell on Azure storage account that is associated with your host VM. This initiates a Cromwell workflow, and returns a workflow id that is appended to the trigger JSON file name and transferred over to the "inprogress" directory in the ""workflows" container.<br/>

Once your workflow completes, you can view the output files in the "cromwell-executions" container within your Azure Storage account. Additional output files from the cromwell endpoint, including metadata and the timing file, are found in the "outputs" container. <br/>
