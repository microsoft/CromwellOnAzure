// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.ComponentModel;
using System.Reflection;

namespace CromwellOnAzureDeployer
{
    internal static class RoleDefinitions
    {
        internal static string GetDisplayName(PropertyInfo property) => property.GetCustomAttribute<DisplayNameAttribute>().DisplayName;

        internal static class General
        {
            // https://learn.microsoft.com/azure/role-based-access-control/built-in-roles/general#contributor
            /// <summary>
            /// Grants full access to manage all resources, but does not allow you to assign roles in Azure RBAC, manage assignments in Azure Blueprints, or share image galleries.
            /// </summary>
            [DisplayName("Contributor")]
            internal static Guid Contributor { get; } = new("b24988ac-6180-42a0-ab88-20f7382dd24c");

            // https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles/general#owner
            /// <summary>
            /// Grants full access to manage all resources, including the ability to assign roles in Azure RBAC.
            /// </summary>
            [DisplayName("Grants full access to manage all resources, including the ability to assign roles in Azure RBAC.")]
            internal static Guid Owner { get; } = new("8e3af657-a8ff-443c-a75c-2fe8c4bcb635");

            internal static PropertyInfo GetProperty(string name) => RoleDefinitions.GetProperty(typeof(General), name);
        }

        internal static class Networking
        {
            // https://learn.microsoft.com/azure/role-based-access-control/built-in-roles/networking#network-contributor
            /// <summary>
            /// Lets you manage networks, but not access to them.
            /// </summary>
            [DisplayName("Network Contributor")]
            internal static Guid NetworkContributor { get; } = new("4d97b98b-1d4f-4787-a291-c67834d212e7");

            internal static PropertyInfo GetProperty(string name) => RoleDefinitions.GetProperty(typeof(Networking), name);
        }

        internal static class Storage
        {
            // https://learn.microsoft.com/azure/role-based-access-control/built-in-roles/storage#storage-blob-data-owner
            /// <summary>
            /// Provides full access to Azure Storage blob containers and data, including assigning POSIX access control. To learn which actions are required for a given data operation, see [Permissions for calling data operations](https://learn.microsoft.com/rest/api/storageservices/authorize-with-azure-active-directory#permissions-for-calling-data-operations)/>.
            /// </summary>
            [DisplayName("Storage Blob Data Owner")]
            internal static Guid StorageBlobDataOwner { get; } = new("b7e6dc6d-f1e8-4753-8033-0f276bb0955b");

            internal static PropertyInfo GetProperty(string name) => RoleDefinitions.GetProperty(typeof(Storage), name);
        }

        internal static class Identity
        {
            // https://learn.microsoft.com/azure/role-based-access-control/built-in-roles/identity#managed-identity-operator
            /// <summary>
            /// Read and Assign User Assigned Identity.
            /// </summary>
            [DisplayName("Managed Identity Operator")]
            internal static Guid ManagedIdentityOperator { get; } = new("f1a07417-d97a-45cb-824c-7a7467783830");

            internal static PropertyInfo GetProperty(string name) => RoleDefinitions.GetProperty(typeof(Identity), name);
        }

        private static PropertyInfo GetProperty(Type type, string name) => type.GetProperty(name) ?? throw new ArgumentException("Property not found.", name);
    }
}
