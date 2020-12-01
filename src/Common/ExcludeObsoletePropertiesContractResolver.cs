// License: https://creativecommons.org/licenses/by-sa/4.0/
// Derived from: https://stackoverflow.com/a/56694483 (author: https://stackoverflow.com/users/10263/brian-rogers)

using System;
using System.Linq;
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
        protected override JsonProperty CreateProperty(MemberInfo member, MemberSerialization memberSerialization)
        {
            var property = base.CreateProperty(member, memberSerialization);

            if (property.AttributeProvider.GetAttributes(inherit: true).OfType<ObsoleteAttribute>().Any())
            {
                property.ShouldSerialize = _ => false;
            }

            return property;
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
