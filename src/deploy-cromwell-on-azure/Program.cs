// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Threading.Tasks;

namespace CromwellOnAzureDeployer
{
    internal class Program
    {
        public static async Task Main(string[] args)
        {
            await InitializeAndDeployAsync(args);
        }

        private static async Task InitializeAndDeployAsync(string[] args)
        {
            var configuration = Configuration.BuildConfiguration(args);

            PrintWelcomeScreen(configuration.Silent);

            var isSuccessful = await new Deployer(configuration).DeployAsync();

            if (isSuccessful)
            {
                Environment.Exit(0);
            }
            else
            {
                Environment.Exit(1);
            }
        }

        private static void PrintWelcomeScreen(bool isSilent)
        {
            if(isSilent)
            {
                return;
            }

            Console.WriteLine("Copyright (c) Microsoft Corporation.");
            Console.WriteLine("Licensed under the MIT License.");
            Console.WriteLine("Privacy & Cookies: https://go.microsoft.com/fwlink/?LinkId=521839");
            Console.WriteLine();
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("Cromwell on Azure");
            Console.ResetColor();
            Console.WriteLine("https://github.com/microsoft/CromwellOnAzure");
            Console.WriteLine();
        }
    }
}
