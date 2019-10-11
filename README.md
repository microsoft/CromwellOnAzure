# Welcome to Cromwell on Azure
## What is Cromwell on Azure? 
[Cromwell](https://cromwell.readthedocs.io/en/stable/) is a workflow management system for scientific workflows, orchestrating the computing tasks needed for genomics analysis. Originally developed by the [Broad Institute](https://github.com/broadinstitute/cromwell), Cromwell is used in the GATK Best Practices genome analysis pipeline. Cromwell supports running scripts at various scales, including your local machine, a local computing cluster, and on the cloud. <br/>

Cromwell on Azure configures all Azure resources needed to run workflows through Cromwell on the Azure cloud, and uses the [GA4GH TES](https://cromwell.readthedocs.io/en/develop/backends/TES/) backend for orchestrating the tasks that create a workflow. The installation sets up a VM host to run the Cromwell server and uses Azure Batch to spin up virtual machines that run each task in a workflow. Cromwell on Azure supports workflows written in the Workflow Description Language or WDL [format](https://cromwell.readthedocs.io/en/stable/LanguageSupport/).<br/>

Get started in a few quick steps! See our [Quickstart](docs/quickstart-cromwell-on-azure.md) guide<br/>
Run Broad Insitute of MIT and Harvard's Best Practices [Genome Analysis Pipeline on Cromwell on Azure](docs/germline-alignment-variantcalling-azure.md)<br/>
Need to debug or configure your Cromwell on Azure runs? See [Advanced configuration](docs/advanced-configuration.md) instructions<br/>
Questions? Check our [FAQs](docs/troubleshooting-guide.md) or open a GitHub issue<br/>