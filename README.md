Dear Community,

After careful consideration, we have decided to shift our focus to new and innovative initiatives that will better serve our community and align with our long-term goals.

**Effective Date**: August 4, 2025

**Impact on Users**:
- The project repository will be archived and set to read-only mode, ensuring that it remains accessible for reference.
- While no further updates, bug fixes, or support will be provided, we encourage you to explore the wealth of knowledge and resources available in the repository.
- Existing issues and pull requests will be closed, but we invite you to engage with other projects and contribute your expertise.
  
**Licensing**: The project will remain under its current open-source license, allowing others to fork and continue development if they choose.


We understand that this change may come as a surprise, but we are incredibly grateful for your support and contributions over the years. Your dedication has been instrumental in the success of this project, and we look forward to your continued involvement in our future endeavors.

Thank you for your understanding and support.


### Announcement: Deployment [changes](https://github.com/microsoft/CromwellOnAzure/wiki/6.0.0-Migration-Guide) in v6.0.0

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
