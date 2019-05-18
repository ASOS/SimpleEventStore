using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using NUnit.Framework;

namespace SimpleEventStore.AzureDocumentDb.Tests
{
    [TestFixture]
    public class AzureDocumentDBEventStoreInitializing
    {
        private const string DatabaseName = "InitializeTests";

        [OneTimeTearDown]
        public async Task TearDownDatabase()
        {
            var client = DocumentClientFactory.Create(DatabaseName);
            await client.DeleteDatabaseAsync(UriFactory.CreateDatabaseUri(DatabaseName));
        }

        [Test]
        public async Task when_initializing_all_expected_resources_are_created()
        {
            var client = DocumentClientFactory.Create(DatabaseName);
            var collectionName = "AllExpectedResourcesAreCreated_" + Guid.NewGuid();
            var storageEngine = await StorageEngineFactory.Create(DatabaseName, o => o.CollectionName = collectionName);
            
            await storageEngine.Initialise();

            var database = (await client.ReadDatabaseAsync(UriFactory.CreateDatabaseUri(DatabaseName))).Resource;
            var collection = (await client.ReadDocumentCollectionAsync(UriFactory.CreateDocumentCollectionUri(DatabaseName, collectionName))).Resource;
            var storedProcedure = (await client.ReadStoredProcedureAsync(UriFactory.CreateStoredProcedureUri(DatabaseName, collectionName, TestConstants.AppendStoredProcedureName))).Resource;
            var offer = client.CreateOfferQuery()
                .Where(r => r.ResourceLink == collection.SelfLink)
                .AsEnumerable()
                .OfType<OfferV2>()
                .Single();

            Assert.That(offer.Content.OfferThroughput, Is.EqualTo(TestConstants.RequestUnits));
            Assert.That(collection.DefaultTimeToLive, Is.Null);
            Assert.That(collection.PartitionKey.Paths.Count, Is.EqualTo(1));
            Assert.That(collection.PartitionKey.Paths.Single(), Is.EqualTo("/streamId"));
            Assert.That(collection.IndexingPolicy.IncludedPaths.Count, Is.EqualTo(1));
            Assert.That(collection.IndexingPolicy.IncludedPaths[0].Path, Is.EqualTo("/*"));
            Assert.That(collection.IndexingPolicy.ExcludedPaths.Count, Is.EqualTo(3));
            Assert.That(collection.IndexingPolicy.ExcludedPaths[0].Path, Is.EqualTo("/body/*"));
            Assert.That(collection.IndexingPolicy.ExcludedPaths[1].Path, Is.EqualTo("/metadata/*"));
        }

        [Test]
        public async Task when_initializing_with_a_time_to_live_it_is_set()
        {
            var ttl = 60;
            var collectionName = "TimeToLiveIsSet_" + Guid.NewGuid();
            var client = DocumentClientFactory.Create(DatabaseName);
            var storageEngine = await StorageEngineFactory.Create(DatabaseName, o => 
            {
                o.CollectionName = collectionName;
                o.DefaultTimeToLive = ttl;
            });

            await storageEngine.Initialise();

            var collection = (await client.ReadDocumentCollectionAsync(UriFactory.CreateDocumentCollectionUri(DatabaseName, collectionName))).Resource;
            Assert.That(collection.DefaultTimeToLive, Is.EqualTo(ttl));
        }
    }
}