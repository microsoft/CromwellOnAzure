using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TesApi.Models;
using TesApi.Web;

namespace TesApi.Tests
{
    [TestClass]
    public class PricingTests
    {
        /// <summary>
        /// This test can be used to get the estimated cost of a workflow
        /// </summary>
        [TestMethod]
        [Ignore]
        private async Task GetEstimatedWorkflowCostAsync()
        {
            // TODO add Cosmos DB connection details here
            Uri cosmosDbEndpoint = null;
            const string cosmosDbKey = null;

            // TODO specify a Cromwell Workflow ID
            const string cromwellWorkflowId = null;

            const string CosmosDbDatabaseId = "TES";
            const string CosmosDbCollectionId = "Tasks";
            const string CosmosDbPartitionId = "01";

            var repo = new CosmosDbRepository<TesTask>(cosmosDbEndpoint, cosmosDbKey, CosmosDbDatabaseId, CosmosDbCollectionId, CosmosDbPartitionId);
            var workflowTasks = (await repo.GetItemsAsync(t => t.Executors.Any(e => e.Command.Any(c => c.Contains(cromwellWorkflowId))), 1000, null)).Item2.ToList();
            decimal totalCost = 0;

            foreach (var task in workflowTasks)
            {
                // Cosmos DB uses current timezone for EndTime
                var taskRuntimeMinutes = (TimeZoneInfo.ConvertTimeToUtc(DateTime.Parse(task.Value.EndTime)) - DateTime.Parse(task.Value.CreationTime)).TotalMinutes;
                totalCost += (decimal)(taskRuntimeMinutes / 60) * task.Value.Resources.VmInfo.PricePerHour;
            }

            var startTime = DateTime.Parse(workflowTasks.OrderBy(t => t.Value.CreationTime).First().Value.CreationTime);
            var endTime = TimeZoneInfo.ConvertTimeToUtc(DateTime.Parse(workflowTasks.OrderByDescending(t => DateTime.Parse(t.Value.EndTime)).First().Value.EndTime));
            var runtime = endTime - startTime;

            Console.WriteLine($"Workflow ID: {cromwellWorkflowId} Runtime: {Math.Floor(runtime.TotalHours):f0}h {runtime.Minutes}m Cost: ${totalCost:n2}");
        }
    }
}
