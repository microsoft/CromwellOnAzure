// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace CromwellOnAzureDeployer
{
    internal class Program
    {
        private static readonly bool StartedFromGui = Console.CursorTop == 0 && Console.CursorLeft == 0;

        public static async Task Main(string[] args)
        {
            await InitializeAndDeployAsync(args);
        }

        private static async Task InitializeAndDeployAsync(string[] args)
        {
            PrintWelcomeScreen();

            var configuration = Configuration.BuildConfiguration(args);

            var services = new ServiceCollection()
                .AddLogging(loggingBuilder => { loggingBuilder.AddConsole(); })
                .BuildServiceProvider();

            await new Deployer(services.GetRequiredService<ILoggerFactory>(), configuration).DeployAsync();
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

            if (StartedFromGui && !Debugger.IsAttached)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("This program requires parameters and must be started from the command line. Press any key to exit...");
                Console.ReadKey();
                Environment.Exit(1);
            }
        }
    }
}
