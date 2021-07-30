namespace SimpleEventStore.CosmosDb.Tests
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos;
    using NUnit.Framework;
    using SimpleEventStore.Tests;
    using SimpleEventStore.Tests.Events;

    [Ignore("Not used in CI/CD pipeline")]
    public class PerformanceTests : EventStoreTestBase
    {
        private const string DatabaseName = "PerformanceTests";
        private readonly CosmosClient client = CosmosClientFactory.Create();

        [OneTimeTearDown]
        public async Task TearDownDatabase()
        {
            await client.GetDatabase(DatabaseName).DeleteAsync();
        }

        protected override Task<IStorageEngine> CreateStorageEngine()
        {
            return CosmosDbStorageEngineFactory.Create(collectionName: "PerformanceTests", databaseName: DatabaseName, builder => builder.UseDatabase(options => options.DatabaseRequestUnits = 100000));
        }

        private static readonly IEnumerable<int> NumberOfClients = Enumerable.Range(10, 100).Where(i => i % 25 == 0);

        [Test]
        public async Task append_as_quickly_as_possible([ValueSource(nameof(NumberOfClients))] int clients)
        {
            const int streamsPerClient = 40;
            const int eventsPerStream = 2;

            var streams = new ConcurrentBag<string>();

            async Task CreateStreams()
            {
                for (var i = 0; i < streamsPerClient; i++)
                {
                    var streamId = Guid.NewGuid().ToString();

                    await this.Subject.AppendToStream(streamId, 0, new EventData(Guid.NewGuid(), new OrderCreated(streamId)));
                    await this.Subject.AppendToStream(streamId, 1, new EventData(Guid.NewGuid(), new OrderDispatched(streamId)));

                    streams.Add(streamId);
                }
            }

            var streamCreatingClients = new List<Task>();
            for (var i = 0; i < clients; i++)
            {
                streamCreatingClients.Add(CreateStreams());
            }

            var stopWatch = Stopwatch.StartNew();
            await Task.WhenAll(streamCreatingClients);
            stopWatch.Stop();
            var timeTaken = stopWatch.Elapsed;

            var writesPerSecond = (clients * streamsPerClient * eventsPerStream) / timeTaken.TotalSeconds;

            Console.WriteLine($"Clients = {clients}");
            Console.WriteLine($"Streams per client = {streamsPerClient}");
            Console.WriteLine($"Total events written = {clients * streamsPerClient * eventsPerStream}");
            Console.WriteLine($"Writes per second = {writesPerSecond}");

            Assert.That(streams.Count, Is.EqualTo(clients * streamsPerClient));
        }
    }
}