using System;

namespace Common.TesModels
{
    public class TesTaskLogMetadataKeyAttribute : Attribute
    {
        public string Name { get; private set; }

        public TesTaskLogMetadataKeyAttribute(string name)
        {
            Name = name;
        }
    }
}
