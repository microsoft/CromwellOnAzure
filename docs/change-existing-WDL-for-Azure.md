# How to modify an existing WDL file to run on Cromwell on Azure

For any pipeline, you can create a [WDL](https://software.broadinstitute.org/wdl/) file that calls your tools in docker containers. Please note that Cromwell on Azure only supports tasks with docker containers defined for security reasons.<br/>

In order to run a WDL file, you must modify/create a workflow with the following runtime attributes for the tasks that are compliant with the [TES or Task Execution Schemas](https://cromwell.readthedocs.io/en/develop/backends/TES/):

```
runtime {
    cpu: 1
    memory: 2 GB
    disk: 10 GB
    docker:
    maxRetries: 0
}
```
Ensure that the attributes `memory` and `disk` (note: use the singular form for `disk` NOT `disks`) have units. Supported units from Cromwell:

> KB - "KB", "K", "KiB", "Ki"<br/>
> MB - "MB", "M", "MiB", "Mi"<br/>
> GB - "GB", "G", "GiB", "Gi"<br/>
> TB - "TB", "T", "TiB", "Ti"<br/>

`preemptible` and `zones` attributes are currently not being passed through Broad's Cromwell to the TES backend, and hence are not supported.<br/>
Each of these runtime attributes are specific to your workflow and tasks within those workflows. The default values for resource requirements are as set above.<br/>
Learn more about Cromwell's runtime attributes [here](https://cromwell.readthedocs.io/en/develop/RuntimeAttributes).

## Runtime attributes comparison with a GCP WDL file

Left panel shows a WDL file created for GCP whereas the right panel is the modified WDL that runs on Azure.


![Runtime Attributes](/docs/screenshots/runtime.PNG)


## Using maxRetries to replace the preemptible attribute

If you want to use the `preemptible` attribute but don’t use `maxRetries` for a task, consider replacing `preemptible` by `maxRetries` to keep the retry functionality. Remember that for Cromwell on Azure, you can either use low-priority VMs in Batch (default configuration) for all tasks or use dedicated VMs for all task by changing the `docker-compose.yml` setting `UsePreemptibleVmsOnly` as described in [this section](/docs/troubleshooting-guide.md/#How-can-I-configure-my-Cromwell-on-Azure-instance-to-use-dedicated-Batch-VMs-to-avoid-getting-preempted?)


![Preemptible Attribute](/docs/screenshots/preemptible.PNG)


## Accompanying index files for BAM or VCF files

If a tool you are using within a task assumes that an index file for your data (BAM or VCF file) is located in the same folder, add an index file to the list of parameters when defining and calling the task to ensure the accompanying index file is copied to the correct location for access:


![Index file parameter](/docs/screenshots/index_1.PNG) 


![Index file called in task](/docs/screenshots/index_2.PNG)


## Calculating disk_size when scattering

In the current implementation, the entire input file is passed to each task created by the WDL `scatter` [operation](https://support.terra.bio/hc/en-us/articles/360037128572?id=6716). If you calculate `disk_size` runtime attribute dynamically within the task, use the full size of input file instead of dividing by the number of shards to allow for enough disk space to perform the task. Do not forget to add size of index files if you added them as parameters:


![Disk size scatter](/docs/screenshots/disk_size_scatter.PNG)
