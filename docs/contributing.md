# Contributing to Cromwell on Azure
Table of Contents
- [Code of Conduct](#code-of-conduct)
- [Issues and Pull Requests](#issues-and-pull-requests)
- [Developer Environment](#developer-environment)
  - [Coding Standards](#coding-standards)
  - [Private VNet](#setting-up-a-private-vnet)

## Code of Conduct 
This project follows the [Microsoft Open Source Code of Conduct](https://github.com/microsoft/CromwellOnAzure/blob/main/CODE_OF_CONDUCT.md). We encourage all contributors to read before jumping in.

## Issues and Pull Requests
- Open issues can be found [here](https://github.com/microsoft/CromwellOnAzure/issues). We also keep a project board of issues to solve by certain releases which can be found [here](https://github.com/microsoft/CromwellOnAzure/projects). Please tag new issues appropriately and loop in maintainers for discussion if you're not sure. 
- If you're working on a large issue, please use the issue templates to break it down into smaller sub tasks. We're looking to balance avoiding massive functionality changing PRs, while not including half-baked features.
- To make sure you're on top of linting, please run Code Cleanup (Ctrl+K, Ctrl+E) on your files in Visual Studio to make use of our `.editorconfig`. To configure this to run automatically on save in Visual Studio, go to Options > Text Editor > Code Cleanup and select Run Code Cleanup profile on save.
- Please include a "risk assessment" in your PR descriptions describing what areas your PR could possibly affect. This helps reviewers less familiar with your code give a better review and anticipate problems.
- Please CC the group Genomics-devs on your PRs, or the appropriate individual(s) from said group.  
- If you're ready for a PR review make sure the PR is not marked as Draft.
- Make sure all Azure changes work with a [Private VNet deployment](#setting-up-a-private-vnet). We're currently working on including this in integration testing so all verification must happen manually.

## Developer Environment
### Getting Started
Instructions for getting your environment set up and building the executable can be found [here](https://github.com/microsoft/CromwellOnAzure#optional-build-the-executable-yourself). Once your build successfully deploys the Azure resources and runs a test workflow you're good to go. 

If you run into any problems or want more information and ways to customize deployments and runs, please see the [Troubleshooting Guide](https://github.com/microsoft/CromwellOnAzure/blob/main/docs/troubleshooting-guide.md). 
### So you're ready to contribute?
You've read the above section on Issues and Pull Requests and you're ready to jump in! We ask that your contributions follow our coding standards and your changes work with a private vnet deployment.

#### Coding Standards 
Cromwell on Azure currently does not have enforced linting, so we do ask all contributors to help keep things neat. We generally follow the [C# Style Conventions](https://docs.microsoft.com/en-us/dotnet/csharp/fundamentals/coding-style/coding-conventions). 

This includes things like 
- Keeping one newline between end braces `}` 
- Using `var` instead of explicitly typed variables
- Keeping our `using` statements in alphabetical order (can be fixed by running Code Cleanup in Visual Studio)
- Cleaning up unused variables
- Making full use of modern C# conventions such as omitting type name or using object initalizer for the [`new` keyword](https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/operators/new-operator#constructor-invocation) and using modern operators such as [is](https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/operators/is) (ex: `vnetAndSubnet is not null` instead of `vnetAndSubnet != null`)
- Using [Asynchronous Programming](https://docs.microsoft.com/en-us/dotnet/csharp/programming-guide/concepts/async/) to effectively allocate resources and block as little as possible.

We also ask contributors to please update the docs if you add a flag to Configuration.cs.

#### Setting up a Private VNet
(TODO: Replace this section with links to issue #286 once that goes in)

Deploying Cromwell on Azure in a Private Virtual Network ensures a secure self-contained deployment with no public endpoints. This deployment expects you to have already created a number of your own resources hooked up to the VNet. 

Azure resources to manually set up (must all be deployed in same region): 
- Virtual Network (with Managed PostgreSQL subnet if applicable) and Network Security Group
- A VM in the virtual network to use as a bridge machine. (You won't be able to run Cromwell on Azure from your local machine with the Private VNet.)
- Azure Batch account with private endpoint
- CosmosDB with private endpoint
- Azure Storage account with private endpoint
- Azure Container Registry with private endpoint. You'll also need to create a Managed Identity (named `<ResourceGroupName>`-identity) and give the ACR read permissions. 

Once you've provisioned the above and you're ready to run Cromwell on Azure, the additional command line flags that must be set in addition to the default required (or set in config.json):
- PrivateNetworking = true
- VnetResourceGroupName
- VnetName 
- SubnetName 
- BatchNodesSubnetId (This will be something like "`/subscriptions/<subscription ID>/resourceGroups/<Resource Group Name>/providers/Microsoft.Network/virtualNetworks/<Virtual Network Name>/subnets/<Subnet Name>`")
- BatchAccountName
- StorageAccountName 
- CosmosDbAccountName
- The image names if you have your own Azure Container Registry (TesImageName, TriggerServiceImageName, DockerInDockerImageName, BlobxferImageName)
