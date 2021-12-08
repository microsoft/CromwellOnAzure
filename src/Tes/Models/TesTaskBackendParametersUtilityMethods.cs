namespace Tes.Models
{
    public partial class TesTask
    {
        /// <summary>
        /// Get the backend parameter value for the specified parameter
        /// </summary>
        /// <returns>The value if it exists; null otherwise</returns>
        public string GetBackendParameterValue(TesResources.SupportedBackendParameters parameter)
        {
            string backendParameterValue = null;

            this?.Resources
                ?.BackendParameters
                ?.TryGetValue(parameter.ToString(), out backendParameterValue);

            return backendParameterValue;
        }

        /// <summary>
        /// Checks if a backend parameter was present
        /// </summary>
        /// <returns>True if the parameter value is not null or whitespace; false otherwise</returns>
        public bool ContainsBackendParameterValue(TesResources.SupportedBackendParameters parameter)
        {
            return !string.IsNullOrWhiteSpace(GetBackendParameterValue(parameter));
        }
    }
}
