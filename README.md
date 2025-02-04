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

## Related Projects

[Genomics Data Analysis with Jupyter Notebooks on Azure](https://github.com/microsoft/genomicsnotebook)<br/>
