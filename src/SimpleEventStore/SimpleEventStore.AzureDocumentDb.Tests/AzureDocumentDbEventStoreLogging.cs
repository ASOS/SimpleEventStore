using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Extensions.Configuration;
using System;
using System.Threading.Tasks;
using NUnit.Framework;
using SimpleEventStore.Tests.Events;

namespace SimpleEventStore.AzureDocumentDb.Tests
{
    [TestFixture]
    public class AzureDocumentDbEventStoreLogging
    {
        [Test]
        public async Task when_a_write_operation_is_successful_the_log_callback_is_called()
        {
            ResponseInformation response = null;
            var sut = new EventStore(await CreateStorageEngine(t => response = t));
            var streamId = Guid.NewGuid().ToString();

            await sut.AppendToStream(streamId, 0, new EventData(Guid.NewGuid(), new OrderCreated("TEST-ORDER")));

            Assert.NotNull(response);
            await TestContext.Out.WriteLineAsync($"Charge: {response.RequestCharge}");
            await TestContext.Out.WriteLineAsync($"Quota Usage: {response.CurrentResourceQuotaUsage}");
            await TestContext.Out.WriteLineAsync($"Max Resource Quote: {response.MaxResourceQuota}");
            await TestContext.Out.WriteLineAsync($"Response headers: {response.ResponseHeaders}");
        }

        [Test]
        public async Task when_a_read_operation_is_successful_the_log_callback_is_called()
        {
            var logCount = 0;
            var sut = new EventStore(await CreateStorageEngine(t => logCount++));
            var streamId = Guid.NewGuid().ToString();

            await sut.AppendToStream(streamId, 0, new EventData(Guid.NewGuid(), new OrderCreated("TEST-ORDER")));
            await sut.ReadStreamForwards(streamId);

            Assert.That(logCount, Is.EqualTo(2));
        }

        private static Task<IStorageEngine> CreateStorageEngine(Action<ResponseInformation> onSuccessCallback, string databaseName = "LoggingTests")
        {
            var config = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json")
                .Build();

            var documentDbUri = config["Uri"];
            var authKey = config["AuthKey"];
            var consistencyLevel = config["ConsistencyLevel"];

            if (!Enum.TryParse(consistencyLevel, true, out ConsistencyLevel consistencyLevelEnum))
            {
                throw new Exception($"The ConsistencyLevel value {consistencyLevel} is not supported");
            }

            DocumentClient client = new DocumentClient(new Uri(documentDbUri), authKey);

            return new AzureDocumentDbStorageEngineBuilder(client, databaseName)
                .UseCollection(o =>
                {
                    o.ConsistencyLevel = consistencyLevelEnum;
                    o.CollectionRequestUnits = 400;
                })
                .UseLogging(o =>
                {
                    o.Success = onSuccessCallback;
                })
                .Build()
                .Initialise();
        }
    }
}
