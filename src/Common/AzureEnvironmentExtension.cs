// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using Microsoft.Azure.Management.ResourceManager.Fluent;

namespace Common
{
    // This class is used for support sovereign cloud.
    // All configuration information please refer: 
    // https://learn.microsoft.com/en-us/azure/china/resources-developer-guide
    public static class AzureEnvironmentExtension
    {
        public static string GetBlobEndPointSuffix(this AzureEnvironment env)
        {
            return ".blob." + env.StorageEndpointSuffix;
        }

        public static string GetAzureRootSuffix(this AzureEnvironment env)
        {
            return env.ResourceManagerEndpoint.Replace("https://management.", string.Empty);
        }

        public static bool IsAvailableEnvironmentName(string envName)
        {
            foreach(var azEnv in AzureEnvironment.KnownEnvironments)
            {
                if (0 == string.Compare(azEnv.Name, envName, true))
                {
                    return true;
                }
            }
            return false;
        }
    }
}

