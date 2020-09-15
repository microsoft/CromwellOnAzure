using System;
using TesApi.Models;

namespace TesApi.Models
{
    /// <summary>
    /// Contains task execution metrics when task is handled by Azure Batch 
    /// </summary>
    public class BatchNodeMetrics
    {
        /// <summary>
        /// BlobXfer docker image pull duration
        /// </summary>
        [TesTaskLogMetadataKey("blobxfer_pull_duration_sec")]
        public double? BlobXferImagePullDurationInSeconds { get; set; }

        /// <summary>
        /// Executor docker image pull duration
        /// </summary>
        [TesTaskLogMetadataKey("executor_pull_duration_sec")]
        public double? ExecutorImagePullDurationInSeconds { get; set; }

        /// <summary>
        /// File download duration
        /// </summary>
        [TesTaskLogMetadataKey("download_duration_sec")]
        public double? FileDownloadDurationInSeconds { get; set; }

        /// <summary>
        /// Main command execution duration
        /// </summary>
        [TesTaskLogMetadataKey("executor_duration_sec")]
        public double? ExecutorDurationInSeconds { get; set; }

        /// <summary>
        /// File upload duration
        /// </summary>
        [TesTaskLogMetadataKey("upload_duration_sec")]
        public double? FileUploadDurationInSeconds { get; set; }

        /// <summary>
        /// Executor image size in GB
        /// </summary>
        [TesTaskLogMetadataKey("executor_image_size_gb")]
        public double? ExecutorImageSizeInGB { get; set; }

        /// <summary>
        /// File download size in GB
        /// </summary>
        [TesTaskLogMetadataKey("file_download_size_gb")]
        public double? FileDownloadSizeInGB { get; set; }

        /// <summary>
        /// File upload size in GB
        /// </summary>
        [TesTaskLogMetadataKey("file_upload_size_gb")]
        public double? FileUploadSizeInGB { get; set; }

        /// <summary>
        /// Max disk usage in GB
        /// </summary>
        [TesTaskLogMetadataKey("max_disk_usage_gb")]
        public double? MaxDiskUsageInGB { get; set; }

        /// <summary>
        /// Max resident memory usage in GB
        /// </summary>
        [TesTaskLogMetadataKey("max_res_mem_usage_gb")]
        public double? MaxResidentMemoryUsageInGB { get; set; }

        /// <summary>
        /// Max disk usage as percent of total
        /// </summary>
        [TesTaskLogMetadataKey("max_disk_usage_pct")]
        public float? MaxDiskUsagePercent { get; set; }

        /// <summary>
        /// Max resident memory usage as percent of total
        /// </summary>
        [TesTaskLogMetadataKey("max_res_mem_usage_pct")]
        public float? MaxResidentMemoryUsagePercent { get; set; }
    }
}
