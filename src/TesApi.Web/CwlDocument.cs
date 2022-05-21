// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace TesApi.Web
{
    /// <summary>
    /// Represents Common Workflow Language (CWL) document structure
    /// </summary>
    public class CwlDocument
    {
        private readonly Regex sizeRegex = new(@"^([0-9\.]+) *(.+)?$");

        private readonly List<MemoryUnit> memoryUnits = new()
        {
            new MemoryUnit(1, "B"),
            new MemoryUnit(1L << 10, "KB", "K", "KiB", "Ki"),
            new MemoryUnit(1L << 20, "MB", "M", "MiB", "Mi"),
            new MemoryUnit(1L << 30, "GB", "G", "GiB", "Gi"),
            new MemoryUnit(1L << 40, "TB", "T", "TiB", "Ti")
        };

        /// <summary>
        /// Tries to create a <see cref="CwlDocument"/> from the provided CWL file content
        /// </summary>
        /// <param name="cwlFileContent">CWL file content</param>
        /// <param name="cwlDocument">Instance of <see cref="CwlDocument"/>, if successful</param>
        /// <returns>True if successful</returns>
        public static bool TryCreate(string cwlFileContent, out CwlDocument cwlDocument)
        {
            try
            {
                cwlDocument = new DeserializerBuilder()
                    .WithNamingConvention(CamelCaseNamingConvention.Instance)
                    .IgnoreUnmatchedProperties()
                    .Build()
                    .Deserialize<CwlDocument>(cwlFileContent);

                return true;
            }
            catch 
            { 
                cwlDocument = null;
                return false; 
            }
        }

        /// <summary>
        /// "Hints" section of the CWL document
        /// </summary>
        public List<Dictionary<string, string>> Hints { get; set; }

        /// <summary>
        /// Returns cpu value if expressed using TES-style naming "cpu", otherwise null
        /// </summary>
        [YamlIgnore]
        public int? Cpu => this.GetResourceRequirementAsInt("cpu");

        /// <summary>
        /// Returns the memory value if expressed using TES-style naming "memory", otherwise null
        /// </summary>
        [YamlIgnore]
        public double? MemoryGb => this.GetSizeInGb("memory");

        /// <summary>
        /// Returns the disk size in GB, derived from CWL resources tmpDirMin, tmpDirMax, outDirMin, and outDirMax. Also supports TES-style naming "disk".
        /// </summary>
        [YamlIgnore]
        public double? DiskGb
        {
            get
            {
                var tmpDir = this.GetSizeInGb("tmpDirMin") ?? this.GetSizeInGb("tmpDirMax");
                var outDir = this.GetSizeInGb("outDirMin") ?? this.GetSizeInGb("outDirMax");

                return this.GetSizeInGb("disk") ?? ((tmpDir ?? outDir) is not null ? (tmpDir ?? 0) + (outDir ?? 0) : default(double?));
            }
        }

        /// <summary>
        /// Returns the preemptible value if expressed using TES-style naming "preemptible", otherwise null
        /// </summary>
        [YamlIgnore]
        public bool? Preemptible => this.GetResourceRequirementAsBool("preemptible");

        private IEnumerable<KeyValuePair<string, string>> ResourceRequirements =>
            this.Hints?
                .FirstOrDefault(d => d.Any(kv => kv.Key.Equals("class", StringComparison.OrdinalIgnoreCase) && kv.Value.Equals("ResourceRequirement", StringComparison.OrdinalIgnoreCase)))?
                .Where(kv => kv.Key != "class");

        private string GetResourceRequirementAsString(string key)
            => this.ResourceRequirements?.FirstOrDefault(kv => kv.Key.Equals(key, StringComparison.OrdinalIgnoreCase)).Value;

        private int? GetResourceRequirementAsInt(string key)
            => int.TryParse(this.GetResourceRequirementAsString(key), out var temp) ? temp : default(int?);

        private bool? GetResourceRequirementAsBool(string key)
            => bool.TryParse(this.GetResourceRequirementAsString(key), out var temp) ? temp : default(bool?);

        private double? GetSizeInGb(string key)
        {
            // value is "X unit", for example "24 GB"
            // if no unit is supplied, default to MB (CWL default)
            var value = this.GetResourceRequirementAsString(key);

            if (string.IsNullOrEmpty(value))
            {
                return null;
            }

            var matchGroups = sizeRegex.Match(value).Groups;
            var size = double.TryParse(matchGroups[1].Value, out var temp) ? temp : default(double?);
            var unit = !string.IsNullOrWhiteSpace(matchGroups[2].Value) ? matchGroups[2].Value.Trim() : "MB";

            if (size is null)
            {
                return null;
            }

            var bytesInUnit = this.memoryUnits.FirstOrDefault(u => u.Suffixes.Any(s => s.Equals(unit, StringComparison.OrdinalIgnoreCase)))?.SizeInBytes;

            return bytesInUnit is not null ? size * bytesInUnit / 1024 / 1024 / 1024 : default;
        }

        private class MemoryUnit
        {
            public double SizeInBytes { get; set; }
            public string[] Suffixes { get; set; }

            public MemoryUnit(double sizeInBytes, params string[] suffixes)
            {
                this.SizeInBytes = sizeInBytes;
                this.Suffixes = suffixes;
            }
        }
    }
}
