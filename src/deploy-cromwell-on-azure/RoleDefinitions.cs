// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Immutable;

namespace CromwellOnAzureDeployer
{
    internal static class RoleDefinitions
    {
        internal static string GetDisplayName(Guid name) => _displayNames[name];

        private record struct GuidAndDisplayName(Guid Name, string DisplayName);
        private static readonly ImmutableDictionary<string, GuidAndDisplayName> _roleDefinitions
            = ImmutableDictionary<string, GuidAndDisplayName>.Empty.AddRange(
                // Add in order of https://learn.microsoft.com/azure/role-based-access-control/built-in-roles, followed by any custom definitions (in a separate subclass).
                [
                    // https://learn.microsoft.com/azure/role-based-access-control/built-in-roles/general#contributor
                    new($"{nameof(General)}.{nameof(General.Contributor)}", new(new("b24988ac-6180-42a0-ab88-20f7382dd24c"), "Contributor")),

                    // https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles/general#owner
                    new($"{nameof(General)}.{nameof(General.Owner)}", new(new("8e3af657-a8ff-443c-a75c-2fe8c4bcb635"), "Grants full access to manage all resources, including the ability to assign roles in Azure RBAC.")),

                    // https://learn.microsoft.com/azure/role-based-access-control/built-in-roles/networking#network-contributor
                    new($"{nameof(Networking)}.{nameof(Networking.NetworkContributor)}", new(new("4d97b98b-1d4f-4787-a291-c67834d212e7"), "Network Contributor")),

                    // https://learn.microsoft.com/azure/role-based-access-control/built-in-roles/storage#storage-blob-data-owner
                    new($"{nameof(Storage)}.{nameof(Storage.StorageBlobDataOwner)}", new(new("b7e6dc6d-f1e8-4753-8033-0f276bb0955b"), "Storage Blob Data Owner")),

                    // https://learn.microsoft.com/azure/role-based-access-control/built-in-roles/identity#managed-identity-operator
                    new($"{nameof(Identity)}.{nameof(Identity.ManagedIdentityOperator)}", new(new("f1a07417-d97a-45cb-824c-7a7467783830"), "Managed Identity Operator")),
                ]);

        private static readonly ImmutableDictionary<Guid, string> _displayNames
            = _roleDefinitions.Values.ToImmutableDictionary(t => t.Name, t => t.DisplayName);

        internal static class General
        {
            /// <summary>
            /// Grants full access to manage all resources, but does not allow you to assign roles in Azure RBAC, manage assignments in Azure Blueprints, or share image galleries.
            /// </summary>
            internal static Guid Contributor { get; } = _roleDefinitions[$"{nameof(General)}.{nameof(Contributor)}"].Name;

            /// <summary>
            /// Grants full access to manage all resources, including the ability to assign roles in Azure RBAC.
            /// </summary>
            internal static Guid Owner { get; } = _roleDefinitions[$"{nameof(General)}.{nameof(Owner)}"].Name;
        }

        internal static class Networking
        {
            /// <summary>
            /// Lets you manage networks, but not access to them.
            /// </summary>
            internal static Guid NetworkContributor { get; } = _roleDefinitions[$"{nameof(Networking)}.{nameof(NetworkContributor)}"].Name;
        }

        internal static class Storage
        {
            /// <summary>
            /// Provides full access to Azure Storage blob containers and data, including assigning POSIX access control. To learn which actions are required for a given data operation, see [Permissions for calling data operations](https://learn.microsoft.com/rest/api/storageservices/authorize-with-azure-active-directory#permissions-for-calling-data-operations)/>.
            /// </summary>
            internal static Guid StorageBlobDataOwner { get; } = _roleDefinitions[$"{nameof(Storage)}.{nameof(StorageBlobDataOwner)}"].Name;
        }

        internal static class Identity
        {
            /// <summary>
            /// Read and Assign User Assigned Identity.
            /// </summary>
            internal static Guid ManagedIdentityOperator { get; } = _roleDefinitions[$"{nameof(Identity)}.{nameof(ManagedIdentityOperator)}"].Name;
        }
    }
}
