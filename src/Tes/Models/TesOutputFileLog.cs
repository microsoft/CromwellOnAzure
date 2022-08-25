﻿// Copyright (c) Microsoft Corporation.
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
using System.Runtime.Serialization;
using System.Text;
using Newtonsoft.Json;

namespace Tes.Models
{
    /// <summary>
    /// OutputFileLog describes a single output file. This describes file details after the task has completed successfully, for logging purposes.
    /// </summary>
    [DataContract]
    public partial class TesOutputFileLog : IEquatable<TesOutputFileLog>
    {
        public TesOutputFileLog()
            => Common.NewtonsoftJsonSafeInit.SetDefaultSettings();

        /// <summary>
        /// URL of the file in storage, e.g. s3://bucket/file.txt
        /// </summary>
        /// <value>URL of the file in storage, e.g. s3://bucket/file.txt</value>
        [DataMember(Name = "url")]
        public string Url { get; set; }

        /// <summary>
        /// Path of the file inside the container. Must be an absolute path.
        /// </summary>
        /// <value>Path of the file inside the container. Must be an absolute path.</value>
        [DataMember(Name = "path")]
        public string Path { get; set; }

        /// <summary>
        /// Size of the file in bytes.
        /// </summary>
        /// <value>Size of the file in bytes.</value>
        [DataMember(Name = "size_bytes")]
        public string SizeBytes { get; set; }

        /// <summary>
        /// Returns the string presentation of the object
        /// </summary>
        /// <returns>String presentation of the object</returns>
        public override string ToString()
            => new StringBuilder()
                .Append("class TesOutputFileLog {\n")
                .Append("  Url: ").Append(Url).Append('\n')
                .Append("  Path: ").Append(Path).Append('\n')
                .Append("  SizeBytes: ").Append(SizeBytes).Append('\n')
                .Append("}\n")
                .ToString();

        /// <summary>
        /// Returns the JSON string presentation of the object
        /// </summary>
        /// <returns>JSON string presentation of the object</returns>
        public string ToJson()
            => JsonConvert.SerializeObject(this, Formatting.Indented);

        /// <summary>
        /// Returns true if objects are equal
        /// </summary>
        /// <param name="obj">Object to be compared</param>
        /// <returns>Boolean</returns>
        public override bool Equals(object obj)
            => obj switch
            {
                var x when x is null => false,
                var x when ReferenceEquals(this, x) => true,
                _ => obj.GetType() == GetType() && Equals((TesOutputFileLog)obj),
            };

        /// <summary>
        /// Returns true if TesOutputFileLog instances are equal
        /// </summary>
        /// <param name="other">Instance of TesOutputFileLog to be compared</param>
        /// <returns>Boolean</returns>
        public bool Equals(TesOutputFileLog other)
            => other switch
            {
                var x when x is null => false,
                var x when ReferenceEquals(this, x) => true,
                _ =>
                (
                    Url == other.Url ||
                    Url is not null &&
                    Url.Equals(other.Url)
                ) &&
                (
                    Path == other.Path ||
                    Path is not null &&
                    Path.Equals(other.Path)
                ) &&
                (
                    SizeBytes == other.SizeBytes ||
                    SizeBytes is not null &&
                    SizeBytes.Equals(other.SizeBytes)
                ),
            };

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
                if (Url is not null)
                {
                    hashCode = hashCode * 59 + Url.GetHashCode();
                }

                if (Path is not null)
                {
                    hashCode = hashCode * 59 + Path.GetHashCode();
                }

                if (SizeBytes is not null)
                {
                    hashCode = hashCode * 59 + SizeBytes.GetHashCode();
                }

                return hashCode;
            }
        }

        #region Operators
#pragma warning disable 1591

        public static bool operator ==(TesOutputFileLog left, TesOutputFileLog right)
            => Equals(left, right);

        public static bool operator !=(TesOutputFileLog left, TesOutputFileLog right)
            => !Equals(left, right);

#pragma warning restore 1591
        #endregion Operators
    }
}
