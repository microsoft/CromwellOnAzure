// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common.HostConfigs;

namespace HostConfigConsole
{
    internal sealed class BuildCmd : Command
    {
        public BuildCmd()
            : base("build", "Builds HostConfig metadata into deployment assets.")
        {
            TreatUnmatchedTokensAsErrors = true;
            AddArgument(hostConfigs);
            AddOption(hostConfigsDirOption);
            AddOption(outFileOption);

            hostConfigsDirOption.AddValidator(result =>
            {
                var value = result.GetValueForOption(hostConfigsDirOption);
                if (value is null)
                {
                    result.ErrorMessage = "--host-configs-directory must exist and be accessible.";
                }
                else if (Uri.TryCreate(value, UriKind.Absolute, out var uri))
                {
                    if (uri.Scheme != "https" && uri.Host != "github.com" || uri.Segments.Length < 3)
                    {
                        result.ErrorMessage = "--host-configs-directory must in a GitHub repository.";
                    }
                }
                else
                {
                    var dir = new DirectoryInfo(value);
                    if (!(dir.Exists))
                    {
                        result.ErrorMessage = "--host-configs-directory must exist and be accessible.";
                    }
                }
            });

            hostConfigs.AddValidator(result =>
            {
                var value = result.GetValueForOption(hostConfigsDirOption);
                if (value is null || Uri.TryCreate(value, UriKind.Absolute, out _))
                {
                    return;
                }

                var dir = new DirectoryInfo(value);
                string? errMsg = default;
                result
                    .GetValueForArgument(hostConfigs)
                    .Where(arg => !Directory.Exists(Path.Combine(dir.FullName, arg)))
                    .ForEach(arg =>
                    {
                        errMsg ??= string.Empty;
                        errMsg += $"Host configuration directory '{arg}' must exist in '{dir.FullName}' and be accessible." + Environment.NewLine;
                    });

                if (errMsg is not null)
                {
                    result.ErrorMessage = errMsg.TrimEnd(Environment.NewLine.ToCharArray());
                }
            });

            Handler = new CommandHandler(Children.ToArray());
        }

        private readonly Argument<string[]> hostConfigs
            = new("host-configs", "Zero or more directory names of host configuration descriptor directories.") { Arity = ArgumentArity.ZeroOrMore };

        private readonly Option<FileInfo> outFileOption
            = new(new[] { "--output", "-o" }, "Path to output file (used to stage metadata for the update command).");

        private readonly Option<string> hostConfigsDirOption
            = new(new[] { "--host-configs-directory", "-d" }, "Path to directory containing all the host configuration descriptor directories to process.") { IsRequired = true };

        private sealed class CommandHandler : CommandHandler<string, FileInfo, string[]>, BaseCommand
        {
            public CommandHandler(params Symbol[] symbols)
                : base(symbols) { }

            protected override async Task InvokeAsync(InvocationContext context, string? hostConfigsPath, FileInfo? outFile, string[]? hostConfigs)
            {
                ArgumentNullException.ThrowIfNull(hostConfigsPath);

                outFile ??= BaseCommand.DefaultHostConfig();
                outFile.Directory?.Create();

                Uri uri = new(hostConfigsPath, UriKind.RelativeOrAbsolute);
                if (!uri.IsAbsoluteUri)
                {
                    uri = new Uri(Path.Combine(Environment.CurrentDirectory, hostConfigsPath), UriKind.Absolute);
                }

                var hostConfigsDir = uri.Scheme switch
                {
                    "file" => FileSystem.GetDirectory(new DirectoryInfo(uri.AbsolutePath)),
                    "https" => await GitHub.GetDirectory(uri),
                    _ => throw new InvalidOperationException("Unsupported hostConfigs URI scheme."),
                };

                var (config, resources, applications) = await new Builder(hostConfigsDir)
                    .Build(context.Console.WriteLine, hostConfigs ?? Array.Empty<string>());

                bool areSame;
                {
                    if (outFile.Exists)
                    {
                        try
                        {
                            using var package = ConfigurationPackageReadable.Open(outFile);
                            areSame = Builder.AreSame(config, package.GetHostConfig() ?? new HostConfig());
                        }
                        catch
                        {
                            areSame = false;
                        }
                    }
                    else
                    {
                        areSame = false;
                    }
                }

                if (!areSame)
                {
                    context.Console.WriteLine("Saving configuration");
                    using var package = ConfigurationPackageWritable.Create(outFile);
                    await Task.WhenAll(resources.Select(async resource => package.SetResource(resource.Name, await resource.Stream)).ToArray());
                    await Task.WhenAll(applications.Select(async app =>
                    {
                        var name = config.PackageHashes.FirstOrDefault(t => t.Value.Equals(app.Version, StringComparison.OrdinalIgnoreCase)).Key;
                        package.SetApplication(name, app.Version, await app.Stream);
                    }).ToArray());

                    package.SetHostConfig(config);
                }

                context.Console.WriteLine("Finished");
            }
        }
    }
}
