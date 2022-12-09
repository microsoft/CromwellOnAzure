// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Tes.Models;
using TesApi.Web;
using TesApi.Web.Management;

namespace TesApi.Tests;

[TestClass]
public class ResourceQuotaVerifierTests
{

    private ResourceQuotaVerifier resourceQuotaVerifier;
    private Mock<IResourceQuotaProvider> quotaProvider;
    private Mock<IAzureProxy> azureProxy;
    private Mock<ILogger> logger;
    private Mock<IBatchSkuInformationProvider> skuInfoProvider;

    private const string Region = "eastus";
    private List<VirtualMachineInformation> vmSizeAndPriceList;

    public ResourceQuotaVerifierTests() { }

    [TestInitialize]
    public void BeforeEach()
    {
        azureProxy = new Mock<IAzureProxy>();
        logger = new Mock<ILogger>();
        quotaProvider = new Mock<IResourceQuotaProvider>();
        skuInfoProvider = new Mock<IBatchSkuInformationProvider>();
        resourceQuotaVerifier = new ResourceQuotaVerifier(azureProxy.Object, quotaProvider.Object, skuInfoProvider.Object, Region, logger.Object);

    }

    [TestMethod]
    [ExpectedException(typeof(InvalidOperationException))]
    public async Task CheckBatchAccountQuotasAsync_ProviderReturnsNull_ThrowsExceptionAndLogsException()
    {
        var vmInfo = new VirtualMachineInformation();

        BatchAccountQuotas batch = null;
        quotaProvider.Setup(p => p.GetBatchAccountQuotaInformationAsync(It.IsAny<VirtualMachineInformation>()))
            .ReturnsAsync(batch);

        await resourceQuotaVerifier.CheckBatchAccountQuotasAsync(vmInfo);

        logger.Verify(l => l.LogError(It.IsAny<string>(), It.IsAny<Exception>()), Times.Once);

    }

    [DataRow(10, 5, 10)] //not enough total quota
    [DataRow(10, 10, 5)] //not enough family quota
    [DataTestMethod]
    [ExpectedException(typeof(AzureBatchLowQuotaException))]
    public async Task CheckBatchAccountQuotasAsync_IsDedicatedNotEnoughCoreQuota_ThrowsAzureBatchLowQuotaException(int requestedNumberOfCores, int totalCoreQuota, int vmFamilyQuota)
    {
        await SetupAndCheckBatchAccountQuotasAsync(requestedNumberOfCores, totalCoreQuota, vmFamilyQuota, 0, 10, 5, 0);
    }

    [DataRow(10, 100, 10, 10, 0, 100)] //too many active jobs
    [DataRow(10, 100, 10, 100, 100, 100)] //too many active pools
    [DataRow(10, 100, 10, 10, 0, 0)] //too total cores in use
    [DataTestMethod]
    [ExpectedException(typeof(AzureBatchQuotaMaxedOutException))]
    public async Task CheckBatchAccountQuotasAsync_IsDedicatedNotEnoughCoreQuota_ThrowsAzureBatchQuotaMaxedOutException(int requestedNumberOfCores, int totalCoreQuota, int activeJobCount, int activeJobAndJobScheduleQuota, int activePoolCount, int poolQuota)
    {
        await SetupAndCheckBatchAccountQuotasAsync(requestedNumberOfCores, totalCoreQuota, 100, activeJobCount, activeJobAndJobScheduleQuota, poolQuota, activePoolCount);
    }

    public async Task SetupAndCheckBatchAccountQuotasAsync(int requestedNumberOfCores, int totalCoreQuota, int vmFamilyQuota, int activeJobCount, int activeJobAndJobScheduleQuota, int poolQuota, int activePoolCount)
    {
        var vmInfo = new VirtualMachineInformation();
        vmInfo.NumberOfCores = requestedNumberOfCores;

        var batchAccountQuotas = new BatchAccountQuotas(totalCoreQuota, vmFamilyQuota, poolQuota, activeJobAndJobScheduleQuota, true);

        quotaProvider.Setup(p => p.GetBatchAccountQuotaInformationAsync(It.IsAny<VirtualMachineInformation>()))
            .ReturnsAsync(batchAccountQuotas);
        azureProxy.Setup(p => p.GetVmSizesAndPricesAsync()).ReturnsAsync(CreateVmSkuList(10));
        azureProxy.Setup(p => p.GetBatchActiveJobCount()).Returns(activeJobCount);
        azureProxy.Setup(p => p.GetBatchActivePoolCount()).Returns(activePoolCount);
        skuInfoProvider.Setup(p => p.GetVmSizesAndPricesAsync(Region)).ReturnsAsync(CreateBatchSupportedVmSkuList(10));

        await resourceQuotaVerifier.CheckBatchAccountQuotasAsync(vmInfo);

    }

    private List<VirtualMachineInformation> CreateBatchSupportedVmSkuList(int maxNumberOfCores)
    {
        return new List<VirtualMachineInformation>()
        {
            new VirtualMachineInformation()
            {
                LowPriority = false,
                MaxDataDiskCount = 1,
                MemoryInGB = 2.0d,
                NumberOfCores = maxNumberOfCores / 2,
                PricePerHour = 1.0m,
                ResourceDiskSizeInGB = 1d,
                VmFamily = "StandardDSeries",
                VmSize = "D4"
            },
            new VirtualMachineInformation()
            {
                LowPriority = false,
                MaxDataDiskCount = 1,
                MemoryInGB = 4.0d,
                NumberOfCores = maxNumberOfCores / 2,
                PricePerHour = 1.0m,
                ResourceDiskSizeInGB = 1d,
                VmFamily = "StandardDSeries",
                VmSize = "D8"
            },
            new VirtualMachineInformation()
            {
                LowPriority = false,
                MaxDataDiskCount = 1,
                MemoryInGB = 2.0d,
                NumberOfCores = maxNumberOfCores / 2,
                PricePerHour = 1.0m,
                ResourceDiskSizeInGB = 1d,
                VmFamily = "StandardDSeries",
                VmSize = "D2"
            }
        };
    }

    private List<VirtualMachineInformation> CreateVmSkuList(int maxNumberOfCores)
    {
        return new List<VirtualMachineInformation>()
        {
            new VirtualMachineInformation()
            {
                LowPriority = false,
                MaxDataDiskCount = 1,
                MemoryInGB = 2.0d,
                NumberOfCores = maxNumberOfCores / 2,
                PricePerHour = 1.0m,
                ResourceDiskSizeInGB = 1d,
                VmFamily = "StandardDSeries",
                VmSize = "D4"
            },
            new VirtualMachineInformation()
            {
                LowPriority = false,
                MaxDataDiskCount = 1,
                MemoryInGB = 2.0d,
                NumberOfCores = maxNumberOfCores / 2,
                PricePerHour = 1.0m,
                ResourceDiskSizeInGB = 1d,
                VmFamily = "StandardDSeries",
                VmSize = "D2"
            }
        };
    }
}


