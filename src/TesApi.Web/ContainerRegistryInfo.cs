// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Web
{
    /// <summary>
    /// Information about the docker images server.
    /// </summary>
    public class ContainerRegistryInfo
    {
        /// <summary>
        /// Gets or sets the registry URL.
        /// </summary>
        public string RegistryServer { get; set; }

        /// <summary>
        /// Gets or sets the password to log into the registry server.
        /// </summary>
        public string Username { get; set; }

        /// <summary>
        /// Gets or sets the user name to log into the registry server.
        /// </summary>
        public string Password { get; set; }
    }
}
