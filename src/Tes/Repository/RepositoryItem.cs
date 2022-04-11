// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using Newtonsoft.Json;

namespace Tes.Repository
{
    /// <summary>
    /// Base class for items stored in <see cref="IRepository{T}"/>. The type must contain a property named "id".
    /// </summary>
    /// <typeparam name="T">Type of the item</typeparam>
    [DataContract]
    public abstract class RepositoryItem<T> where T : class
    {
        /// <summary>
        /// Name of the JSON field containing the partition key
        /// </summary>
        public const string PartitionKeyFieldName = "_partitionKey";

        private const string IdFieldName = "id";
        private const string EtagFieldName = "_etag";
        private static readonly Func<RepositoryItem<T>, string> GetIdStatic = GetIdFunc();

        /// <summary>
        /// Returns the Id of the underlying object
        /// </summary>
        /// <returns>Id of the underlying object</returns>
        public string GetId() => GetIdStatic(this);

        /// <summary>
        /// Partition key
        /// </summary>
        [DataMember(Name = PartitionKeyFieldName)]
        public string PartitionKey { get; set; }

        /// <summary>
        /// Etag for concurrency control
        /// </summary>
        [DataMember(Name = EtagFieldName)]
        public string ETag { get; set; }

        private static Func<RepositoryItem<T>, string> GetIdFunc()
        {
            var idProperty = typeof(T).GetProperties()
                .FirstOrDefault(info =>
                    (info.GetCustomAttribute<DataMemberAttribute>(false)?.Name?.Equals(IdFieldName) ?? false)
                    || (info.GetCustomAttribute<JsonPropertyAttribute>(false)?.PropertyName.Equals(IdFieldName) ?? false)
                    || info.Name.Equals(IdFieldName));

            if (idProperty is not null)
            {
                return obj => idProperty.GetValue(obj)?.ToString();
            }

            var idField = typeof(T).GetFields()
                .FirstOrDefault(info =>
                    (info.GetCustomAttribute<DataMemberAttribute>(false)?.Name?.Equals(IdFieldName) ?? false)
                    || (info.GetCustomAttribute<JsonPropertyAttribute>(false)?.PropertyName.Equals(IdFieldName) ?? false)
                    || info.Name.Equals(IdFieldName));

            if (idField is not null)
            {
                return obj => idField.GetValue(obj)?.ToString();
            }

            throw new Exception($"Type used for RepositoryItem, {typeof(T).Name}, does not contain required field/property named or decorated with DataMemberAttribute/JsonPropertyAttribute '{IdFieldName}'.");
        }
    }
}
