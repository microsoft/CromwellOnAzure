using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Microsoft.Azure.Management.ResourceManager.Fluent.Core;

namespace TesApi.Web
{
    /// <summary>
    /// Utility class to work with Azure Region names
    /// </summary>
    public static class AzureRegionUtils
    {
        private static Dictionary<string, string> regionAltNameLookup = new Dictionary<string, string>(
                typeof(Region)
                .GetFields(BindingFlags.Public | BindingFlags.Static)
                .Where(f => f.Name != "GovernmnetUSIowa") // https://github.com/Azure/azure-libraries-for-net/issues/874
                .Select(f => new KeyValuePair<string, string>(f.GetValue(null).ToString().ToLowerInvariant(), f.Name)));

        /// <summary>
        /// Gets the 'alt' Azure region name with no spaces, from a canonical Azure region name
        /// </summary>
        /// <param name="canonicalName">Azure region name, e.g. 'westus'</param>
        /// <returns>The 'alt' name for an Azure region with no spaces, e.g. 'USWest'</returns>
        public static string GetAltName(string canonicalName)
        {
            if (regionAltNameLookup.TryGetValue(canonicalName.ToLowerInvariant(), out var altName))
            {
                return altName;
            }
            else
            {
                throw new ArgumentException($"The region name '{canonicalName}' is unknown.  If this is a new Azure region, the Microsoft.Azure.Management.ResourceManager.Fluent.Core package likely needs to be updated.");
            }
        }
    }
}
