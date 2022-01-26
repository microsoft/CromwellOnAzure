// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Reflection;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using Tes.Models;

namespace TesApi.Web
{
    /// <summary>
    /// A constract resolver for <see cref="TesTask"/>s to facilitate swagger codegen
    /// </summary>
    public class MinimalTesTaskContractResolver : DefaultContractResolver
    {
        // MINIMAL: Task message will include ONLY the fields: Task.Id Task.State
        private static readonly List<Tuple<Type, string>> PropertiesToInclude = new()
        {
                Tuple.Create(typeof(TesTask), nameof(TesTask.Id)),
                Tuple.Create(typeof(TesTask), nameof(TesTask.State)),
                Tuple.Create(typeof(TesListTasksResponse), nameof(TesListTasksResponse.Tasks)),
                Tuple.Create(typeof(TesListTasksResponse), nameof(TesListTasksResponse.NextPageToken))
            };

        /// <summary>
        /// Singleton instance
        /// </summary>
        public static readonly MinimalTesTaskContractResolver Instance = new();

        /// <summary>
        /// Creates a new JsonProperty based on System.Reflection
        /// </summary>
        /// <param name="member">Member metadata</param>
        /// <param name="memberSerialization">JSON serialization options</param>
        /// <returns></returns>
        protected override JsonProperty CreateProperty(MemberInfo member, MemberSerialization memberSerialization)
        {
            var property = base.CreateProperty(member, memberSerialization);

            if (!PropertiesToInclude.Contains(Tuple.Create(property.DeclaringType, property.UnderlyingName)))
            {
                property.ShouldSerialize = instance => false;
            }

            return property;
        }
    }
}
