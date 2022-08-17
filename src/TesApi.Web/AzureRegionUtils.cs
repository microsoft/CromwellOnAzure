// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Generic;

namespace TesApi.Web
{
    /// <summary>
    /// Utility class to work with Azure Region names
    /// </summary>
    public static class AzureRegionUtils
    {
        private static readonly Dictionary<string, string> billingRegionLookup = new(
            new List<KeyValuePair<string, string>> {
                new KeyValuePair<string, string>("australiacentral", "AU Central"),
                new KeyValuePair<string, string>("australiacentral2", "AU Central 2"),
                new KeyValuePair<string, string>("australiaeast", "AU East"),
                new KeyValuePair<string, string>("australiasoutheast", "AU Southeast"),
                new KeyValuePair<string, string>("brazilsouth", "BR South"),
                new KeyValuePair<string, string>("canadacentral", "CA Central"),
                new KeyValuePair<string, string>("canadaeast", "CA East"),
                new KeyValuePair<string, string>("centralindia", "IN Central"),
                new KeyValuePair<string, string>("centralus", "US Central"),
                new KeyValuePair<string, string>("eastasia", "AP East"),
                new KeyValuePair<string, string>("eastus", "US East"),
                new KeyValuePair<string, string>("eastus2", "US East 2"),
                new KeyValuePair<string, string>("francecentral", "FR Central"),
                new KeyValuePair<string, string>("francesouth", "FR South"),
                new KeyValuePair<string, string>("germanynorth", "DE North"),
                new KeyValuePair<string, string>("germanywestcentral", "DE West Central"),
                new KeyValuePair<string, string>("japaneast", "JA East"),
                new KeyValuePair<string, string>("japanwest", "JA West"),
                new KeyValuePair<string, string>("koreacentral", "KR Central"),
                new KeyValuePair<string, string>("koreasouth", "KR South"),
                new KeyValuePair<string, string>("northcentralus", "US North Central"),
                new KeyValuePair<string, string>("northeurope", "EU North"),
                new KeyValuePair<string, string>("norwayeast", "NO East"),
                new KeyValuePair<string, string>("norwaywest", "NO West"),
                new KeyValuePair<string, string>("southafricanorth", "ZA North"),
                new KeyValuePair<string, string>("southafricawest", "ZA West"),
                new KeyValuePair<string, string>("southcentralus", "US South Central"),
                new KeyValuePair<string, string>("southeastasia", "AP Southeast"),
                new KeyValuePair<string, string>("southindia", "IN South"),
                new KeyValuePair<string, string>("switzerlandnorth", "CH North"),
                new KeyValuePair<string, string>("switzerlandwest", "CH West"),
                new KeyValuePair<string, string>("uaecentral", "AE Central"),
                new KeyValuePair<string, string>("uaenorth", "AE North"),
                new KeyValuePair<string, string>("uknorth", "UK North"),
                new KeyValuePair<string, string>("uksouth", "UK South"),
                new KeyValuePair<string, string>("uksouth2", "UK South 2"),
                new KeyValuePair<string, string>("ukwest", "UK West"),
                new KeyValuePair<string, string>("westcentralus", "US West Central"),
                new KeyValuePair<string, string>("westeurope", "EU West"),
                new KeyValuePair<string, string>("westindia", "IN West"),
                new KeyValuePair<string, string>("westus", "US West"),
                new KeyValuePair<string, string>("westus2", "US West 2"),
            });

        /// <summary>
        /// Gets the Azure billing region name from an Azure ARM location
        /// </summary>
        /// <param name="armLocation">Azure ARM location, e.g. 'westus'</param>
        /// <param name="billingRegionName">The Azure billing region name, e.g. 'US West'</param>
        /// <returns>true if a matching billing region is found, false otherwise</returns>
        public static bool TryGetBillingRegionName(string armLocation, out string billingRegionName)
        {
            billingRegionName = null;

            if (billingRegionLookup.TryGetValue(armLocation.ToLowerInvariant(), out var altName))
            {
                billingRegionName = altName;
                return true;
            }

            return false;
        }
    }
}
