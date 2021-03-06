// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

/*
 * Task Execution Service
 *
 * No description provided (generated by Openapi Generator https://github.com/openapitools/openapi-generator)
 *
 * OpenAPI spec version: 0.3.0
 * 
 * Generated by: https://openapi-generator.tech
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using Newtonsoft.Json;

namespace Tes.Models
{
    /// <summary>
    /// Resources describes the resources requested by a task.
    /// </summary>
    [DataContract]
    public partial class TesResources : IEquatable<TesResources>
    {
        /// <summary>
        /// Requested number of CPUs
        /// </summary>
        /// <value>Requested number of CPUs</value>
        [DataMember(Name = "cpu_cores")]
        public long? CpuCores { get; set; }

        /// <summary>
        /// Is the task allowed to run on preemptible compute instances (e.g. AWS Spot)?
        /// </summary>
        /// <value>Is the task allowed to run on preemptible compute instances (e.g. AWS Spot)?</value>
        [DataMember(Name = "preemptible")]
        public bool? Preemptible { get; set; }

        /// <summary>
        /// Requested RAM required in gigabytes (GB)
        /// </summary>
        /// <value>Requested RAM required in gigabytes (GB)</value>
        [DataMember(Name = "ram_gb")]
        public double? RamGb { get; set; }

        /// <summary>
        /// Requested disk size in gigabytes (GB)
        /// </summary>
        /// <value>Requested disk size in gigabytes (GB)</value>
        [DataMember(Name = "disk_gb")]
        public double? DiskGb { get; set; }

        /// <summary>
        /// Request that the task be run in these compute zones.
        /// </summary>
        /// <value>Request that the task be run in these compute zones.</value>
        [DataMember(Name = "zones")]
        public List<string> Zones { get; set; }

        /// <summary>
        /// Returns the string presentation of the object
        /// </summary>
        /// <returns>String presentation of the object</returns>
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append("class TesResources {\n");
            sb.Append("  CpuCores: ").Append(CpuCores).Append("\n");
            sb.Append("  Preemptible: ").Append(Preemptible).Append("\n");
            sb.Append("  RamGb: ").Append(RamGb).Append("\n");
            sb.Append("  DiskGb: ").Append(DiskGb).Append("\n");
            sb.Append("  Zones: ").Append(Zones).Append("\n");
            sb.Append("}\n");
            return sb.ToString();
        }

        /// <summary>
        /// Returns the JSON string presentation of the object
        /// </summary>
        /// <returns>JSON string presentation of the object</returns>
        public string ToJson()
        {
            return JsonConvert.SerializeObject(this, Formatting.Indented);
        }

        /// <summary>
        /// Returns true if objects are equal
        /// </summary>
        /// <param name="obj">Object to be compared</param>
        /// <returns>Boolean</returns>
        public override bool Equals(object obj)
        {
            if (obj is null)
            {
                return false;
            }

            if (ReferenceEquals(this, obj))
            {
                return true;
            }

            return obj.GetType() == GetType() && Equals((TesResources)obj);
        }

        /// <summary>
        /// Returns true if TesResources instances are equal
        /// </summary>
        /// <param name="other">Instance of TesResources to be compared</param>
        /// <returns>Boolean</returns>
        public bool Equals(TesResources other)
        {
            if (other is null)
            {
                return false;
            }

            if (ReferenceEquals(this, other))
            {
                return true;
            }

            return
                (
                    CpuCores == other.CpuCores ||
                    CpuCores != null &&
                    CpuCores.Equals(other.CpuCores)
                ) &&
                (
                    Preemptible == other.Preemptible ||
                    Preemptible != null &&
                    Preemptible.Equals(other.Preemptible)
                ) &&
                (
                    RamGb == other.RamGb ||
                    RamGb != null &&
                    RamGb.Equals(other.RamGb)
                ) &&
                (
                    DiskGb == other.DiskGb ||
                    DiskGb != null &&
                    DiskGb.Equals(other.DiskGb)
                ) &&
                (
                    Zones == other.Zones ||
                    Zones != null &&
                    Zones.SequenceEqual(other.Zones)
                );
        }

        /// <summary>
        /// Gets the hash code
        /// </summary>
        /// <returns>Hash code</returns>
        public override int GetHashCode()
        {
            unchecked // Overflow is fine, just wrap
            {
                var hashCode = 41;
                // Suitable nullity checks etc, of course :)
                if (CpuCores != null)
                {
                    hashCode = hashCode * 59 + CpuCores.GetHashCode();
                }

                if (Preemptible != null)
                {
                    hashCode = hashCode * 59 + Preemptible.GetHashCode();
                }

                if (RamGb != null)
                {
                    hashCode = hashCode * 59 + RamGb.GetHashCode();
                }

                if (DiskGb != null)
                {
                    hashCode = hashCode * 59 + DiskGb.GetHashCode();
                }

                if (Zones != null)
                {
                    hashCode = hashCode * 59 + Zones.GetHashCode();
                }

                return hashCode;
            }
        }

        #region Operators
#pragma warning disable 1591

        public static bool operator ==(TesResources left, TesResources right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(TesResources left, TesResources right)
        {
            return !Equals(left, right);
        }

#pragma warning restore 1591
        #endregion Operators
    }
}
