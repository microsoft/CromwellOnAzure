# Germline alignment and variant calling pipeline
This tutorial walks through how to run the germline alignment and variant calling pipeline, based on [Best Practices Genome Analysis Pipeline by Broad Institute of MIT and Harvard](https://software.broadinstitute.org/gatk/best-practices/workflow?id=11165), on Cromwell on Azure. 

> This WDL pipeline implements data pre-processing and initial variant calling (GVCF generation) according to the GATK Best Practices (June 2016) for germline SNP and Indel discovery in human whole-genome sequencing data.

Learn more [here](https://github.com/microsoft/five-dollar-genome-analysis-pipeline-azure)

### Input data 

All the required input files for the tutorial are on a publicly hosted Azure Storage account.

You can find the sample WDL, JSON inputs and the `WholeGenomeGermlineSingleSample.hg38.json` trigger file for the pipeline on the [GitHub repo](https://github.com/microsoft/five-dollar-genome-analysis-pipeline-azure). 

You can use the "msgenpublicdata" storage account directly as a relative path, like in the [JSON inputs file](https://raw.githubusercontent.com/microsoft/five-dollar-genome-analysis-pipeline-azure/master-azure/WholeGenomeGermlineSingleSample.hg38.inputs.json).

This is an example trigger JSON file:
```
{
 "WorkflowUrl": "https://raw.githubusercontent.com/microsoft/five-dollar-genome-analysis-pipeline-azure/az1.1.0/WholeGenomeGermlineSingleSample.wdl",
 "WorkflowInputsUrl": "https://raw.githubusercontent.com/microsoft/five-dollar-genome-analysis-pipeline-azure/az1.1.0/WholeGenomeGermlineSingleSample.hg38.inputs.json",
 "WorkflowOptionsUrl": null,
 "WorkflowDependenciesUrl": null
}
```
See more instructions on how to run a workflow on Cromwell on Azure [here](quickstart-cromwell-on-azure.md).

## Start a WDL workflow
To start a WDL workflow, go to your Cromwell on Azure Storage account associated with your host VM. In the "workflows" container, create the directory "new" and place the trigger file in that folder. This initiates a Cromwell workflow, and returns a workflow id that is appended to the trigger JSON file name and transferred over to the "inprogress" directory in the Workflows container.<br/>

![directory](/docs/screenshots/newportal.PNG)
![directory2](/docs/screenshots/newexplorer.PNG)

For example, a trigger JSON file with name `task1.json` in the "new" directory, will be move to "inprogress" directory with a modified name `task1.guid.json`. This guid is a workflow id assigned by Cromwell.<br/>

Once your workflow completes, you can view the output files of your workflow in the "cromwell-executions" container within your Azure Storage Account. Additional output files from the cromwell endpoint, including metadata and the timing file, are found in the "outputs" container. To learn more about Cromwell's metadata and timing information, visit the [Cromwell documentation](https://cromwell.readthedocs.io/en/stable/).<br/>
