using System;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using SimpleEventStore.Tests;

namespace SimpleEventStore.AzureDocumentDb.Tests
{
    internal class DatabaseConstants
    {
        internal const string DatabaseName = "ReadingTestsWithBucket";
    }

    public class AzureDocumentDbEventStoreReadingWithBucketSeparation : EventStoreReadingWithBucketSeparation
    {
        protected override Task<IStorageEngine> CreateStorageEngine(string bucket)
        {
            return StorageEngineFactory.Create(DatabaseConstants.DatabaseName, bucket);
        }

        protected override async Task AppendDocument(StorageEvent document)
        {
            var storageEngine = (AzureDocumentDbStorageEngine) await StorageEngineFactory.Create(DatabaseConstants.DatabaseName);
            var client = storageEngine.Client;

            var docDbEvent = new DocumentDbStorageEventWithoutBucket
            {
                Id = $"{document.StreamId}:{document.EventNumber}",
                EventId = document.EventId,
                Body = JObject.FromObject(document.EventBody),
                BodyType = storageEngine.TypeMap.GetNameFromType(document.EventBody.GetType()),
                StreamId = document.StreamId,
                EventNumber = document.EventNumber
            };

            await client.CreateDocumentAsync(storageEngine.CommitsLink, docDbEvent);
        }

        private class DocumentDbStorageEventWithoutBucket
        {
            [JsonProperty("id")]
            public string Id { get; set; }

            [JsonProperty("eventId")]
            public Guid EventId { get; set; }

            [JsonProperty("body")]
            public JObject Body { get; set; }

            [JsonProperty("bodyType")]
            public string BodyType { get; set; }

            [JsonProperty("streamId")]
            public string StreamId { get; set; }

            [JsonProperty("eventNumber")]
            public int EventNumber { get; set; }

        }
    }
}