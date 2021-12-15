// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Security.Cryptography;
using System.Text.RegularExpressions;

namespace CromwellOnAzureDeployer
{
    public static class Utility
    {
        /// <summary>
        /// Generates a secure password with one lowercase letter, one uppercase letter, and one number
        /// </summary>
        /// <param name="length">Length of the password</param>
        /// <returns>The password</returns>
        public static string GeneratePassword(int length = 16)
        {
            // one lower, one upper, one number, min length
            var regex = new Regex(@"^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)[a-zA-Z\d]{" + length.ToString() + ",}$");

            while (true)
            {
                using var rng = new RNGCryptoServiceProvider();
                var buffer = new byte[length];
                rng.GetBytes(buffer);

                var password = Convert.ToBase64String(buffer)
                    .Replace("+", "-")
                    .Replace("/", "_")
                    .Substring(0, length);

                if (regex.IsMatch(password))
                {
                    return password;
                }
            }
        }
    }
}
