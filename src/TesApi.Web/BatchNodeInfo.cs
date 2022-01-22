namespace TesApi.Web
{
    /// <summary>
    /// Batch node image and sku info
    /// </summary>
    public class BatchNodeInfo
    {
        /// <summary>
        /// Azure Batch image offer
        /// </summary>
        public string BatchImageOffer { get; set; }
        /// <summary>
        /// Azure Batch image publisher
        /// </summary>
        public string BatchImagePublisher { get; set; }
        /// <summary>
        /// Azure Batch image SKU
        /// </summary>
        public string BatchImageSku { get; set; }
        /// <summary>
        /// Azure Batch image version
        /// </summary>
        public string BatchImageVersion { get; set; }
        /// <summary>
        /// Azure Batch node agent sku ID
        /// </summary>
        public string BatchNodeAgentSkuId { get; set; }
    }
}
