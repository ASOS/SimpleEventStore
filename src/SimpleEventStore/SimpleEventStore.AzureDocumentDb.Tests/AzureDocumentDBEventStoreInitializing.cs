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
        private const string DatabaseName = "EventStoreTests-Initialize";
        private readonly Uri databaseUri = UriFactory.CreateDatabaseUri(DatabaseName);
        private readonly DocumentClient client = DocumentClientFactory.Create();

        [TearDown]
        public Task TearDownDatabase()
        {
            return client.DeleteDatabaseAsync(databaseUri);
        }

        [Test]
        public async Task when_initializing_all_expected_resources_are_created()
        {
            var collectionName = "AllExpectedResourcesAreCreated_" + Guid.NewGuid();
            var storageEngine = await InitialiseStorageEngine(collectionName, collectionThroughput: TestConstants.RequestUnits);

            var database = (await client.ReadDatabaseAsync(databaseUri)).Resource;
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
            var storageEngine = await StorageEngineFactory.Create(collectionName, DatabaseName, x =>
            {
                x.UseCollection(o => o.DefaultTimeToLive = ttl);
            });

            var collection =
                (await client.ReadDocumentCollectionAsync(
                    UriFactory.CreateDocumentCollectionUri(DatabaseName, collectionName))).Resource;
            Assert.That(collection.DefaultTimeToLive, Is.EqualTo(ttl));
        }

        [Test]
        public async Task when_using_shared_throughput_it_is_set_at_a_database_level()
        {
            const int dbThroughput = 800;
            var collectionName = "SharedCollection_" + Guid.NewGuid();

            var storageEngine = await InitialiseStorageEngine(collectionName, dbThroughput: dbThroughput);

            Assert.AreEqual(dbThroughput, await GetDatabaseThroughput());
            Assert.AreEqual(null, await GetCollectionThroughput(collectionName));
        }


        [Test]
        public async Task when_throughput_is_set_offer_is_updated()
        {
            var dbThroughput = 800;
            var collectionThroughput = 400;
            var collectionName = "UpdateThroughput_" + Guid.NewGuid();

            await InitialiseStorageEngine(collectionName, collectionThroughput, dbThroughput);

            Assert.AreEqual(dbThroughput, await GetDatabaseThroughput());
            Assert.AreEqual(collectionThroughput, await GetCollectionThroughput(collectionName));

            dbThroughput = 1600;
            collectionThroughput = 800;

            await InitialiseStorageEngine(collectionName, collectionThroughput, dbThroughput);

            Assert.AreEqual(dbThroughput, await GetDatabaseThroughput());
            Assert.AreEqual(collectionThroughput, await GetCollectionThroughput(collectionName));
        }

        [TestCase(null, null, null, 400)]
        [TestCase(600, null, 600, null)]
        [TestCase(null, 600, null, 600)]
        [TestCase(600, 600, 600, 600)]
        [TestCase(600, 1000, 600, 1000)]
        [TestCase(1000, 600, 1000, 600)]
        public async Task set_database_and_collection_throughput_when_database_has_not_been_created(int? dbThroughput, int? collectionThroughput, int? expectedDbThroughput, int? expectedCollectionThroughput)
        {
            var collectionName = "CollectionThroughput_" + Guid.NewGuid();


            var storageEngine = await InitialiseStorageEngine(collectionName, collectionThroughput, dbThroughput);

            Assert.AreEqual(expectedDbThroughput, await GetDatabaseThroughput());
            Assert.AreEqual(expectedCollectionThroughput, await GetCollectionThroughput(collectionName));
        }


        [TestCase(null, 500, null)]
        [TestCase(1000, 500, 1000)]
        public async Task set_database_and_collection_throughput_when_database_has_already_been_created(int? collectionThroughput, int? expectedDbThroughput, int? expectedCollectionThroughput)
        {
            const int existingDbThroughput = 500;
            await CreateDatabase(existingDbThroughput);
            var collectionName = "CollectionThroughput_" + Guid.NewGuid();

            var storageEngine = await InitialiseStorageEngine(collectionName, collectionThroughput, null);

            Assert.AreEqual(expectedDbThroughput, await GetDatabaseThroughput());
            Assert.AreEqual(expectedCollectionThroughput, await GetCollectionThroughput(collectionName));
        }

        private static async Task<IStorageEngine> InitialiseStorageEngine(string collectionName, int? collectionThroughput = null,
            int? dbThroughput = null)
        {
            var storageEngine = await StorageEngineFactory.Create(collectionName, DatabaseName, x => {
                x.UseCollection(o => o.CollectionRequestUnits = collectionThroughput);
                x.UseDatabase(o => o.DatabaseRequestUnits = dbThroughput);
            });

            return await storageEngine.Initialise();
        }

        public async Task<int?> GetCollectionThroughput(string collectionName)
        {
            var collection = await client.ReadDocumentCollectionAsync(UriFactory.CreateDocumentCollectionUri(DatabaseName, collectionName));

            var collectionOffer = client.CreateOfferQuery().Where(x => x.ResourceLink == collection.Resource.SelfLink)
                .AsEnumerable().FirstOrDefault();

            return ((OfferV2) collectionOffer)?.Content.OfferThroughput;
        }

        public async Task<int?> GetDatabaseThroughput()
        {
            var db = await client.ReadDatabaseAsync(databaseUri);
            var dbOffer = client.CreateOfferQuery().Where(x => x.ResourceLink == db.Resource.SelfLink).AsEnumerable()
                .FirstOrDefault();

            return ((OfferV2)dbOffer)?.Content.OfferThroughput;
        }

        private Task CreateDatabase(int databaseRequestUnits)
        {
            return client.CreateDatabaseIfNotExistsAsync(
                new Database { Id = DatabaseName },
                new RequestOptions
                {
                    OfferThroughput = databaseRequestUnits
                });
        }

    }
}