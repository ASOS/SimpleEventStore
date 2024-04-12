using System;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using NUnit.Framework;

namespace SimpleEventStore.CosmosDb.Tests
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
            await InitialiseStorageEngine(collectionName, collectionThroughput: TestConstants.RequestUnits);

            var database = client.GetDatabase(DatabaseName);
            var collection = database.GetContainer(collectionName);

            var collectionResponse = await collection.ReadContainerAsync();
            var collectionProperties = collectionResponse.Resource;
            
            var offer = await collection.ReadThroughputAsync();

            Assert.That(offer, Is.EqualTo(TestConstants.RequestUnits));
            Assert.That(collectionProperties.DefaultTimeToLive, Is.Null);
            Assert.That(collectionProperties.PartitionKeyPath, Is.EqualTo("/streamId"));
            Assert.That(collectionProperties.IndexingPolicy.IncludedPaths.Count, Is.EqualTo(1));
            Assert.That(collectionProperties.IndexingPolicy.IncludedPaths[0].Path, Is.EqualTo("/*"));
            Assert.That(collectionProperties.IndexingPolicy.ExcludedPaths.Count, Is.EqualTo(3));
            Assert.That(collectionProperties.IndexingPolicy.ExcludedPaths[0].Path, Is.EqualTo("/body/*"));
            Assert.That(collectionProperties.IndexingPolicy.ExcludedPaths[1].Path, Is.EqualTo("/metadata/*"));
        }

        [Test]
        public async Task when_using_shared_throughput_it_is_set_at_a_database_level()
        {
            const int dbThroughput = 800;
            var collectionName = "SharedCollection_" + Guid.NewGuid();
            
            await InitialiseStorageEngine(collectionName, dbThroughput: dbThroughput);

            Assert.That(dbThroughput, Is.EqualTo(await GetDatabaseThroughput()));
            Assert.That(await GetCollectionThroughput(collectionName), Is.Null);
        }

        [TestCase(60)]
        [TestCase(10)]
        [TestCase(90)]
        public async Task when_initializing_with_a_time_to_live_it_is_set(int ttl)
        {
            var collectionName = "TimeToLiveIsSet_" + Guid.NewGuid();
            var storageEngine = await CosmosDbStorageEngineFactory.Create(collectionName, DatabaseName,
                x =>
                {
                    x.UseCollection(o => o.DefaultTimeToLive = ttl);
                });

            var collection = await client.GetContainer(DatabaseName, collectionName).ReadContainerAsync();

            var collectionProperties = collection.Resource;

            Assert.That(collectionProperties.DefaultTimeToLive, Is.EqualTo(ttl));
        }


        [Test]
        public async Task when_throughput_is_set_offer_is_updated()
        {
            var dbThroughput = 800;
            var collectionThroughput = 400;
            var collectionName = "UpdateThroughput_" + Guid.NewGuid();

            await InitialiseStorageEngine(collectionName, collectionThroughput, dbThroughput);

            Assert.That(dbThroughput, Is.EqualTo(await GetDatabaseThroughput()));
            Assert.That(collectionThroughput, Is.EqualTo(await GetCollectionThroughput(collectionName)));

            dbThroughput = 1600;
            collectionThroughput = 800;

            await InitialiseStorageEngine(collectionName, collectionThroughput, dbThroughput);

            Assert.That(dbThroughput, Is.EqualTo(await GetDatabaseThroughput()));
            Assert.That(collectionThroughput, Is.EqualTo(await GetCollectionThroughput(collectionName)));
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

            await InitialiseStorageEngine(collectionName, collectionThroughput, dbThroughput);

            Assert.That(expectedDbThroughput, Is.EqualTo(await GetDatabaseThroughput()));
            Assert.That(expectedCollectionThroughput, Is.EqualTo(await GetCollectionThroughput(collectionName)));
        }


        [TestCase(null, 500, null)]
        [TestCase(1000, 500, 1000)]
        public async Task set_database_and_collection_throughput_when_database_has_already_been_created(int? collectionThroughput, int? expectedDbThroughput, int? expectedCollectionThroughput)
        {
            const int existingDbThroughput = 500;
            await CreateDatabase(existingDbThroughput);
            var collectionName = "CollectionThroughput_" + Guid.NewGuid();

            await InitialiseStorageEngine(collectionName, collectionThroughput, null);

            Assert.That(expectedDbThroughput, Is.EqualTo(await GetDatabaseThroughput()));
            Assert.That(expectedCollectionThroughput, Is.EqualTo(await GetCollectionThroughput(collectionName)));
        }

        private static async Task<IStorageEngine> InitialiseStorageEngine(string collectionName, int? collectionThroughput = null,
            int? dbThroughput = null)
        {
            var storageEngine = await CosmosDbStorageEngineFactory.Create(collectionName, DatabaseName, x => {
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