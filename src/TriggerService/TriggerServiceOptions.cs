namespace TriggerService
{
    public class TriggerServiceOptions
    {
        public const string TriggerServiceOptionsSectionName = "TriggerService";
        public string CromwellUrl { get; set; }
        public string DefaultStorageAccountName { get; set; }
        public int MainRunIntervalMilliseconds { get; set; }
        public int AvailabilityCheckIntervalMilliseconds { get; set; }
    }
}
