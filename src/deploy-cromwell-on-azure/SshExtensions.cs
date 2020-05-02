// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Common;
using Renci.SshNet;

namespace CromwellOnAzureDeployer
{
    public static class SshExtensions
    {
        // TODO: cancellationToken
        public static Task UploadFileAsync(this SftpClient sftpClient, Stream input, string path, bool canOverride = true, CancellationToken cancellationToken = default)
        {
            return Task.Factory.FromAsync(sftpClient.BeginUploadFile(input, path, canOverride, null, null), sftpClient.EndUploadFile);
        }

        public static async Task<(string output, string error, int exitStatus)> ExecuteCommandAsync(this SshClient sshClient, string commandText, bool throwOnNonZeroExitCode = false, CancellationToken cancellationToken = default)
        {
            using var sshCommand = sshClient.CreateCommand(commandText);
            var output = await Task.Factory.FromAsync(sshCommand.BeginExecute(), sshCommand.EndExecute);

            if (throwOnNonZeroExitCode && sshCommand.ExitStatus != 0)
            {
                throw new Exception($"ExecuteCommandAsync failed: ExitStatus = {sshCommand.ExitStatus}, Error = '{sshCommand.Error}'");
            }

            return (sshCommand.Result.Trim(), sshCommand.Error, sshCommand.ExitStatus);
        }

        public static void ConnectWithRetries(this SshClient sshClient)
        {
            Retry.Do(() => sshClient.Connect(), retryInterval: TimeSpan.FromSeconds(5), retryCount: 5);
        }
    }
}
