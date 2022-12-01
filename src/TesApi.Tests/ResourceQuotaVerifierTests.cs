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
    private List<VirtualMachineInformation> vmSizeAndPriceList;

    public ResourceQuotaVerifierTests() { }

    [TestInitialize]
    public void BeforeEach()
    {
        azureProxy = new Mock<IAzureProxy>();
        logger = new Mock<ILogger>();
        quotaProvider = new Mock<IResourceQuotaProvider>();
        resourceQuotaVerifier = new ResourceQuotaVerifier(azureProxy.Object, quotaProvider.Object, logger.Object);

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

    [DataRow(10, 5, 10, 0, 0, 0)] //not enough total quota
    [DataRow(10, 10, 11, 5, 0, 0)] //not enough family quota
    [DataRow(10, 5, 11, 11, 10, 10)] //not enough active active job and job schedule quota
    [DataTestMethod]
    [ExpectedException(typeof(AzureBatchLowQuotaException))]
    public async Task CheckBatchAccountQuotasAsync_IsDedicatedNotEnoughCoreQuota_ThrowsException(int requestedNumberOfCores, int totalCoreQuota, int vmFamilyQuota, int activeJobCount, int activeJobAndJobScheduleQuota)
    {
        var vmInfo = new VirtualMachineInformation();
        vmInfo.NumberOfCores = requestedNumberOfCores;

        var batchAccountQuotas = new BatchAccountQuotas(totalCoreQuota, vmFamilyQuota, 5, 5, true);

        quotaProvider.Setup(p => p.GetBatchAccountQuotaInformationAsync(It.IsAny<VirtualMachineInformation>()))
            .ReturnsAsync(batchAccountQuotas);
        azureProxy.Setup(p => p.GetVmSizesAndPricesAsync()).ReturnsAsync(CreateVmSkuList(10));
        azureProxy.Setup(p => p.GetBatchActivePoolCount()).Returns(activeJobCount);


        await resourceQuotaVerifier.CheckBatchAccountQuotasAsync(vmInfo);

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


