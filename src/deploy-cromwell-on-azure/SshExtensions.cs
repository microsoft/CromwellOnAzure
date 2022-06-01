// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Polly;
using Polly.Retry;
using Renci.SshNet;
using Renci.SshNet.Common;

namespace CromwellOnAzureDeployer
{
    public static class SshExtensions
    {
        private static readonly RetryPolicy retryPolicy = Policy
            .Handle<Exception>(ex => ! (ex is SshAuthenticationException && ex.Message.StartsWith("Permission")))
            .WaitAndRetry(10, retryAttempt => TimeSpan.FromSeconds(10));

        // TODO: cancellationToken
        public static Task UploadFileAsync(this SftpClient sftpClient, Stream input, string path, bool canOverride = true)
            => Task.Factory.FromAsync(sftpClient.BeginUploadFile(input, path, canOverride, null, null), sftpClient.EndUploadFile);

        public static async Task<(string output, string error, int exitStatus)> ExecuteCommandAsync(this SshClient sshClient, string commandText)
        {
            using var sshCommand = sshClient.CreateCommand(commandText);
            var output = await Task.Factory.FromAsync(sshCommand.BeginExecute(), sshCommand.EndExecute);

            if (sshCommand.ExitStatus != 0)
            {
                throw new Exception($"ExecuteCommandAsync failed: Command: = {commandText} ExitStatus = {sshCommand.ExitStatus}, Error = '{sshCommand.Error}'");
            }

            return (sshCommand.Result.Trim(), sshCommand.Error, sshCommand.ExitStatus);
        }

        public static void ConnectWithRetries(this SshClient sshClient)
            => retryPolicy.Execute(() => sshClient.Connect());
    }
}
