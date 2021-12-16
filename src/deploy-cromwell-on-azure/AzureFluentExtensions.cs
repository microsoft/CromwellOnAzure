// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using Microsoft.Rest.Azure;
using Newtonsoft.Json;

namespace CromwellOnAzureDeployer
{
    public static class AzureFluentExtensions
    {
        public static CloudError ToCloudError(this CloudException cloudException)
        {
            var content = cloudException?.Response?.Content;

            if (content is null)
            {
                return null;
            }

            try
            {
                return JsonConvert.DeserializeObject<CloudErrorWrapper>(content)?.Error;
            }
            catch
            {
                return null;
            }
        }

        public static CloudErrorType ToCloudErrorType(this CloudException cloudException)
        {
            var code = cloudException.ToCloudError()?.Code;
            _ = Enum.TryParse(code, out CloudErrorType cloudErrorType);
            return cloudErrorType;
        }
    }
    public class CloudErrorWrapper
    {
        public CloudError Error { get; set; }
    }
    public enum CloudErrorType
    {
        NotSet, ExpiredAuthenticationToken, AuthorizationFailed, SkuNotAvailable
    }
}
