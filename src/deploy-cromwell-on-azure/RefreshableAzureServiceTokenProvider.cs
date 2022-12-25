// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.Rest;

namespace CromwellOnAzureDeployer
{
    /// <summary>
    /// ITokenProvider implementation based on AzureServiceTokenProvider from Microsoft.Azure.Services.AppAuthentication package.
    /// </summary>
    public class RefreshableAzureServiceTokenProvider : ITokenProvider
    {
        private readonly string resource;
        private readonly string tenantId;
        private readonly AzureServiceTokenProvider tokenProvider;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="resource">Resource to request tokens for</param>
        /// <param name="tenantId">AAD tenant ID containing the resource</param>
        /// <param name="azureAdInstance">AAD instance to request tokens from</param>
        public RefreshableAzureServiceTokenProvider(string resource, string tenantId = null, string azureAdInstance = "https://login.microsoftonline.com/")
        {
            if (string.IsNullOrEmpty(resource))
            {
                throw new ArgumentException(null, nameof(resource));
            }

            this.resource = resource;
            this.tenantId = tenantId;

            this.tokenProvider = new AzureServiceTokenProvider("RunAs=Developer; DeveloperTool=AzureCli", azureAdInstance: azureAdInstance);
        }

        /// <summary>
        /// Gets the authentication header with token.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Authentication header with token</returns>
        public async Task<AuthenticationHeaderValue> GetAuthenticationHeaderAsync(CancellationToken cancellationToken)
        {
            // AzureServiceTokenProvider caches tokens internally and refreshes them before expiry.
            // This method usually gets called on every request to set the authentication header. This ensures that we cache tokens, and also that we always get a valid one.
            var token = await tokenProvider.GetAccessTokenAsync(resource, tenantId, cancellationToken).ConfigureAwait(false);
            return new AuthenticationHeaderValue("Bearer", token);
        }
    }
}
