// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;

namespace CromwellOnAzureDeployer
{
        /// <summary>
        /// Represents a container to mount with blob-csi-driver. 
        /// </summary>
        public class MountableContainer
        {
            private string sasToken;

            public string StorageAccount { get; set; }
            public string ContainerName { get; set; }
            public string ResourceGroupName { get; set; }
            public string SasToken 
            { 
                get => sasToken; 
                set => sasToken = value.TrimStart('?'); 
            }

            public MountableContainer() { }

            public MountableContainer(Dictionary<string, string> dictionary)
            {
                if (dictionary.ContainsKey("accountName"))
                {
                    StorageAccount = dictionary["accountName"];
                }

                if (dictionary.ContainsKey("containerName"))
                {
                    ContainerName = dictionary["containerName"];
                }

                if (dictionary.ContainsKey("resourceGroup"))
                {
                    ResourceGroupName = dictionary["resourceGroup"];
                }

                if (dictionary.ContainsKey("sasToken"))
                {
                    SasToken = dictionary["sasToken"];
                }
            }

            public Dictionary<string, string> ToDictionary()
            {
                var dictionary = new Dictionary<string, string>();

                if (StorageAccount is not null)
                {
                    dictionary["accountName"] = StorageAccount;
                }

                if (ContainerName is not null)
                {
                    dictionary["containerName"] = ContainerName;
                }

                if (ResourceGroupName is not null)
                {
                    dictionary["resourceGroup"] = ResourceGroupName;
                }

                if (SasToken is not null)
                {
                    dictionary["sasToken"] = SasToken;
                }

                return dictionary;
            }

            public override bool Equals(object other)
            {
                return string.Equals(StorageAccount, ((MountableContainer)other).StorageAccount, StringComparison.OrdinalIgnoreCase) &&
                    string.Equals(ContainerName, ((MountableContainer)other).ContainerName, StringComparison.OrdinalIgnoreCase) &&
                    string.Equals(SasToken, ((MountableContainer)other).SasToken, StringComparison.OrdinalIgnoreCase) &&
                    string.Equals(ResourceGroupName, ((MountableContainer)other).ResourceGroupName, StringComparison.OrdinalIgnoreCase);
            }

            public override int GetHashCode()
            {
                if (SasToken is null)
                {
                    return HashCode.Combine(StorageAccount.GetHashCode(), ContainerName.GetHashCode(), ResourceGroupName.GetHashCode());
                }
                else
                {
                    return HashCode.Combine(StorageAccount.GetHashCode(), ContainerName.GetHashCode(), SasToken.GetHashCode());
                }
            }
        }
}
