// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Reflection;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using Tes.Models;
using Tes.Repository;

namespace TesApi.Web
{
    /// <summary>
    /// Contractor resolver for <see cref="TesTask"/>s
    /// </summary>
    public class BasicTesTaskContractResolver : DefaultContractResolver
    {
        // In BASIC view, task message will include all fields EXCEPT:   
        // Task.ExecutorLog.stdout   
        // Task.ExecutorLog.stderr
        // Input.content
        // TaskLog.system_logs
        // plus additional custom fields added to support running TES with Cromwell on Azure
        private static readonly List<Tuple<Type, string>> PropertiesToSkip = new()
        {
                Tuple.Create(typeof(TesExecutorLog), nameof(TesExecutorLog.Stdout)),
                Tuple.Create(typeof(TesExecutorLog), nameof(TesExecutorLog.Stderr)),
                Tuple.Create(typeof(TesInput), nameof(TesInput.Content)),
                Tuple.Create(typeof(TesTaskLog), nameof(TesTaskLog.SystemLogs)),
                Tuple.Create(typeof(TesTask), nameof(TesTask.IsCancelRequested)),
                Tuple.Create(typeof(TesTask), nameof(TesTask.ErrorCount)),
                Tuple.Create(typeof(TesTask), nameof(TesTask.EndTime)),
                Tuple.Create(typeof(TesTask), nameof(TesTask.WorkflowId)),
                Tuple.Create(typeof(RepositoryItem<TesTask>), nameof(RepositoryItem<TesTask>.ETag)),
                Tuple.Create(typeof(RepositoryItem<TesTask>), nameof(RepositoryItem<TesTask>.PartitionKey))
            };

        /// <summary>
        /// Instance of the resolver
        /// </summary>
        public static readonly BasicTesTaskContractResolver Instance = new();

        /// <summary>
        /// Overridden CreateProperty in order to skip specific properties
        /// </summary>

        protected override JsonProperty CreateProperty(MemberInfo member, MemberSerialization memberSerialization)
        {
            var property = base.CreateProperty(member, memberSerialization);

            if (PropertiesToSkip.Contains(Tuple.Create(property.DeclaringType, property.UnderlyingName)))
            {
                property.ShouldSerialize = instance => false;
            }

            return property;
        }
    }
}
