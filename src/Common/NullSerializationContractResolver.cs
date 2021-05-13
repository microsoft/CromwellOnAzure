using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace Common
{
    public class NullSerializationContractResolver: DefaultContractResolver
    {
        protected override IList<JsonProperty> CreateProperties(Type type, MemberSerialization memberSerialization)
        {
            //Select Properties if not decorated with DoNotSerializeIfNull attribute or 
            //if the property is decorated then it has a value other than null.
            var properties = base.CreateProperties(type, memberSerialization);
            var fieldsInfo = type.GetFields();
            var  filteredproperties = fieldsInfo.Where(f =>
            f.CustomAttributes.Count() == 0 
            ||
            (f.CustomAttributes.Any(y =>
            y.AttributeType == typeof(DoNotSerializeIfNull)) 
            && 
            f.GetValue(null) != null)).ToList();
            return properties.Where(p =>
            filteredproperties.Any(f => f.Name.ToLower().Equals(p.PropertyName.ToLower()) == true)).ToList();
        }
    }
}
