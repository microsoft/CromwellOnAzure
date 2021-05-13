using System;
namespace Common
{
    [AttributeUsage(
        AttributeTargets.Property,
        AllowMultiple = false,
        Inherited = true)]
    public class DoNotSerializeIfNull: Attribute
    {
        public DoNotSerializeIfNull()
        { }
    }
}
