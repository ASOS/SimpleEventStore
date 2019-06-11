using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using NUnit.Framework;
using SimpleEventStore.Tests.Events;

namespace SimpleEventStore.AzureDocumentDb.Tests
{
    [TestFixture]
    public class AzureDocumentDbEventStoreReadingPartiallyDeletedStreams
    {
        [Test]
        public async Task when_reading_a_stream_that_has_deleted_events_the_stream_can_still_be_read()
        {
            const string databaseName = "ReadingPartialStreamTests";
            const string collectionName = "Commits";

            var client = DocumentClientFactory.Create(databaseName);
            var storageEngine = await StorageEngineFactory.Create(databaseName, o => o.CollectionName = collectionName);
            var eventStore = new EventStore(storageEngine);
            var streamId = Guid.NewGuid().ToString();
            await eventStore.AppendToStream(streamId, 0, new EventData(Guid.NewGuid(), new OrderCreated(streamId)), new EventData(Guid.NewGuid(), new OrderDispatched(streamId)));
            await SimulateTimeToLiveExpiration(databaseName, collectionName, client, streamId);

            var stream = await eventStore.ReadStreamForwards(streamId);

            Assert.That(stream.Count, Is.EqualTo(1));
            Assert.That(stream.First().EventBody, Is.InstanceOf<OrderDispatched>());
            Assert.That(stream.First().EventNumber, Is.EqualTo(2));
        }

        private static async Task SimulateTimeToLiveExpiration(string databaseName, string collectionName, DocumentClient client, string streamId)
        {
            await client.DeleteDocumentAsync(
                UriFactory.CreateDocumentUri(databaseName, collectionName, $"{streamId}:1"),
                new RequestOptions() { PartitionKey = new PartitionKey(streamId) });
        }
    }
}
