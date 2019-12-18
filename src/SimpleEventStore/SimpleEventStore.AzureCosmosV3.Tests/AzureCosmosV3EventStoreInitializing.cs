using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Microsoft.Azure.Cosmos.Scripts;
using Microsoft.Azure.Documents;
using NUnit.Framework;

namespace SimpleEventStore.AzureCosmosV3.Tests
{
    [TestFixture]
    public class AzureCosmosV3EventStoreInitializing
    {
        private const string DatabaseName = "EventStoreTests-Initialize-CosmosV3";
        private readonly CosmosClient client = CosmosClientFactory.Create();

        [TearDown]
        public Task TearDownDatabase()
        {
            return client.GetDatabase(DatabaseName).DeleteAsync();
        }

        [Test]
        public async Task when_initializing_all_expected_resources_are_created()
        {
            var collectionName = "AllExpectedResourcesAreCreated_" + Guid.NewGuid();
            var storageEngine = await InitialiseStorageEngine(collectionName, collectionThroughput: TestConstants.RequestUnits);

            var database = client.GetDatabase(DatabaseName);
            var collection = database.GetContainer(collectionName);

            const string queryText = "SELECT * FROM s";

            var storedProcedures = await collection.Scripts.GetStoredProcedureQueryIterator<StoredProcedureProperties>(
                queryText).ReadNextAsync();

            var expectedStoredProcedure = storedProcedures.Resource.FirstOrDefault(s => s.Id.StartsWith("appendToStream-"));

            var offer = await collection.ReadThroughputAsync();

            Assert.That(expectedStoredProcedure, Is.Not.Null);
            Assert.That(offer, Is.EqualTo(TestConstants.RequestUnits));
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
            var storageEngine = await CosmosV3StorageEngineFactory.Create(collectionName, DatabaseName, x => {
                x.UseCollection(o => o.CollectionRequestUnits = collectionThroughput);
                x.UseDatabase(o => o.DatabaseRequestUnits = dbThroughput);
            });

            return await storageEngine.Initialise();
        }

        public Task<int?> GetCollectionThroughput(string collectionName)
        {
            var collection = client.GetContainer(DatabaseName, collectionName);
            return collection.ReadThroughputAsync();
        }

        public Task<int?> GetDatabaseThroughput()
        {
            return client.GetDatabase(DatabaseName).ReadThroughputAsync();
        }

        private Task CreateDatabase(int databaseRequestUnits)
        {
            return client.CreateDatabaseIfNotExistsAsync(DatabaseName, databaseRequestUnits);
        }

    }
}