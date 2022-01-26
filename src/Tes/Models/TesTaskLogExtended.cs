// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using Newtonsoft.Json;
using Tes.Extensions;

namespace Tes.Models
{
    /// <summary>
    /// TaskLog describes logging information related to a Task.
    /// </summary>
    public partial class TesTaskLog
    {
        // Get all TesTaskLog properties that are decorated with TeskTaskLogMetadataKey attribute, recursively 
        private static readonly List<(string MetadataKey, PropertyInfo PropertyInfo)> propertiesBackedByMetadata = typeof(TesTaskLog).GetProperties()
            .Select(prop => prop.PropertyType)
            .Distinct()
            .Append(typeof(TesTaskLog))
            .SelectMany(t => t.GetProperties())
            .Select(prop => new { MetadataKey = prop.GetCustomAttribute<TesTaskLogMetadataKeyAttribute>(false)?.Name, PropertyInfo = prop })
            .Where(kp => kp.MetadataKey is not null)
            .Select(kp => (kp.MetadataKey, kp.PropertyInfo)).ToList();

        /// <summary>
        /// Virtual Machine used by Azure Batch to handle the task
        /// </summary>
        [JsonIgnore]
        public VirtualMachineInformation VirtualMachineInfo { get; set; }

        /// <summary>
        /// Contains task execution metrics when task is handled by Azure Batch
        /// </summary>
        [JsonIgnore]
        public BatchNodeMetrics BatchNodeMetrics { get; set; }

        /// <summary>
        /// Overall reason of the task failure, populated when task execution ends in EXECUTOR_ERROR or SYSTEM_ERROR, for example "DiskFull".
        /// </summary>
        [JsonIgnore]
        [TesTaskLogMetadataKey("failure_reason")]
        public string FailureReason { get; set; }

        /// <summary>
        /// Warning that gets populated if task encounters an issue that needs user's attention.
        /// </summary>
        [JsonIgnore]
        [TesTaskLogMetadataKey("warning")]
        public string Warning { get; set; }

        /// <summary>
        /// Cromwell-specific result code, populated when Batch task execution ends in COMPLETED, containing the exit code of the inner Cromwell script.
        /// </summary>
        [JsonIgnore]
        [TesTaskLogMetadataKey("cromwell_rc")]
        public int? CromwellResultCode { get; set; }

        /// <summary>
        /// Before serialization, store the object properties decorated with TeskTaskLogMetadataKey attribute to Metadata dictionary
        /// </summary>
        /// <param name="context"></param>
        [OnSerializing]
        internal void StoreObjectsToMetadata(StreamingContext context)
        {
            StoreObjectToMetadata(this);
            StoreObjectToMetadata(this.VirtualMachineInfo);
            StoreObjectToMetadata(this.BatchNodeMetrics);
        }

        /// <summary>
        /// After deserialization, use Metadata dictionary to set object properties decorated with TeskTaskLogMetadataKey attribute
        /// </summary>
        /// <param name="context"></param>
        [OnDeserialized]
        internal void LoadObjectsFromMetadata(StreamingContext context)
        {
            LoadObjectFromMetadata(this);
            this.VirtualMachineInfo = TryGetObjectFromMetadata<VirtualMachineInformation>(out var vmInfo) ? vmInfo : null;
            this.BatchNodeMetrics = TryGetObjectFromMetadata<BatchNodeMetrics>(out var metrics) ? metrics : null;
        }

        /// <summary>
        /// Store object properties decorated with TeskTaskLogMetadataKey attribute to the Metadata dictionary
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="obj"></param>
        private void StoreObjectToMetadata<T>(T obj)
        {
            foreach (var property in propertiesBackedByMetadata.Where(prop => prop.PropertyInfo.ReflectedType == typeof(T)))
            {
                var propertyValue = obj is not null ? property.PropertyInfo.GetValue(obj) : null;
                AddOrUpdateMetadataItem(property.MetadataKey, propertyValue);
            }
        }

        /// <summary>
        /// Populate existing object's properties decorated with TeskTaskLogMetadataKey attribute with the values from the Metadata dictionary
        /// </summary>
        /// <typeparam name="T">Type of object</typeparam>
        /// <param name="obj">Object to populate</param>
        private void LoadObjectFromMetadata<T>(T obj)
        {
            foreach (var property in propertiesBackedByMetadata.Where(prop => prop.PropertyInfo.ReflectedType == typeof(T)))
            {
                if(this.TryGetMetadataValue(property.MetadataKey, property.PropertyInfo.PropertyType, out var metadataValue))
                {
                    property.PropertyInfo.SetValue(obj, metadataValue);
                }
            }
        }

        /// <summary>
        /// Create new object of the given type using values from the Metadata dictionary if metadata contains any values for that type.
        /// </summary>
        /// <typeparam name="T">Type of object to populate and return</typeparam>
        /// <returns>Populated object</returns>
        private bool TryGetObjectFromMetadata<T>(out T obj) where T : new()
        {
            if(this.Metadata is null)
            {
                obj = default;
                return false;
            }

            var properties = propertiesBackedByMetadata.Where(prop => prop.PropertyInfo.ReflectedType == typeof(T));
            var metadataKeyNames = properties.Select(x => x.MetadataKey);

            if (this.Metadata.Keys.Intersect(metadataKeyNames).Any())
            {
                obj = new T();
                LoadObjectFromMetadata(obj);
                return true;
            }

            obj = default;
            return false;
        }

        /// <summary>
        /// Get single value from the Metadata dictionary, deserialized into the given type
        /// </summary>
        /// <param name="key">Metadata item key</param>
        /// <param name="type">Type to deserialize to</param>
        /// <param name="result">Deserialized object</param>
        /// <returns>True if metadata value is found.</returns>
        private bool TryGetMetadataValue(string key, Type type, out object result)
        {
            string value = null;
            var hasValue = this.Metadata is not null && this.Metadata.TryGetValue(key, out value);
            result = hasValue ? JsonConvert.DeserializeObject($"\"{value}\"", type) : default;
            return hasValue;
        }

        /// <summary>
        /// Add or update single item in the Metadata dictionary.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key">Metadata item key</param>
        /// <param name="value">Metadata item value</param>
        private void AddOrUpdateMetadataItem<T>(string key, T value)
        {
            if (value is not null)
            {
                this.GetOrAddMetadata()[key] = JsonConvert.SerializeObject(value).Trim('"');
            }
            else if (this.Metadata is not null && this.Metadata.ContainsKey(key))
            {
                this.Metadata.Remove(key);
            }
        }
    }
}
