// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Threading.Tasks;

namespace CromwellOnAzureDeployer
{
    internal class Program
    {
        public static async Task Main(string[] args)
            => await InitializeAndDeployAsync(args);

        private static async Task InitializeAndDeployAsync(string[] args)
        {
            Configuration configuration = null;

            try
            {
                configuration = Configuration.BuildConfiguration(args);
            }
            catch (ArgumentException ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(ex.Message);
                Console.ResetColor();
                Environment.Exit(1);
            }

            if (!configuration.Silent)
            {
                PrintWelcomeScreen();
            }

            Environment.Exit(await new Deployer(configuration).DeployAsync());
        }

        private static void PrintWelcomeScreen()
        {
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
