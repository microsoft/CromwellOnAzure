// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.	// Licensed under the MIT License.

using System;
using System.Reflection;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace Common
{
    public class ExcludeObsoletePropertiesContractResolver : DefaultContractResolver
    {
        /// <summary>
        /// Prevents serialization of properties that are marked obsolete
        /// </summary>
        protected override JsonProperty CreateProperty(MemberInfo memberInfo, MemberSerialization memberSerialization)
        {
            var jsonProperty = base.CreateProperty(memberInfo, memberSerialization);

            foreach (var attribute in jsonProperty.AttributeProvider.GetAttributes(inherit: true))
            {
                if (attribute.GetType() == typeof(ObsoleteAttribute))
                {
                    jsonProperty.ShouldSerialize = _ => false;
                    break;
                }
            }

            return jsonProperty;
        }

        public static JsonSerializerSettings GetSettings()
        {
            return new JsonSerializerSettings
            {
                ContractResolver = new ExcludeObsoletePropertiesContractResolver(),
                Formatting = Formatting.Indented
            };
        }
    }
}
