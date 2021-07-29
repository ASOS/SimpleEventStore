using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;
using NUnit.Framework;
using System;
using System.Linq;
using System.Threading.Tasks;

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
        public async Task when_initializing_manual_scale_all_expected_resources_are_created()
        {
            const int collectionThroughput = 800;
            var collectionName = "AllExpectedResourcesAreCreated_" + Guid.NewGuid();
            var storageEngine = await InitialiseStorageEngine(collectionName, collectionThroughput: collectionThroughput);

            var database = client.GetDatabase(DatabaseName);
            var collection = database.GetContainer(collectionName);

            const string queryText = "SELECT * FROM s";

            var storedProcedures = await collection.Scripts.GetStoredProcedureQueryIterator<StoredProcedureProperties>(queryText).ReadNextAsync();

            var expectedStoredProcedure = storedProcedures.Resource.FirstOrDefault(s => s.Id.StartsWith("appendToStream-"));

            var collectionResponse = await collection.ReadContainerAsync();
            var collectionProperties = collectionResponse.Resource;

            var offer = await collection.ReadThroughputAsync();

            Assert.That(expectedStoredProcedure, Is.Not.Null);
            Assert.That(offer, Is.EqualTo(collectionThroughput));
            Assert.That(collectionProperties.DefaultTimeToLive, Is.Null);
            Assert.That(collectionProperties.PartitionKeyPath, Is.EqualTo("/streamId"));
            Assert.That(collectionProperties.IndexingPolicy.IncludedPaths.Count, Is.EqualTo(1));
            Assert.That(collectionProperties.IndexingPolicy.IncludedPaths[0].Path, Is.EqualTo("/*"));
            Assert.That(collectionProperties.IndexingPolicy.ExcludedPaths.Count, Is.EqualTo(3));
            Assert.That(collectionProperties.IndexingPolicy.ExcludedPaths[0].Path, Is.EqualTo("/body/*"));
            Assert.That(collectionProperties.IndexingPolicy.ExcludedPaths[1].Path, Is.EqualTo("/metadata/*"));
        }

        [Test]
        public async Task when_initializing_auto_scale_all_expected_resources_are_created()
        {
            const int collectionThroughput = 5000;
            var collectionName = "AllExpectedResourcesAreCreated_" + Guid.NewGuid();
            var storageEngine = await InitialiseStorageEngine(collectionName, collectionThroughput: collectionThroughput, autoScale: true);

            var database = client.GetDatabase(DatabaseName);
            var collection = database.GetContainer(collectionName);

            const string queryText = "SELECT * FROM s";

            var storedProcedures = await collection.Scripts.GetStoredProcedureQueryIterator<StoredProcedureProperties>(queryText).ReadNextAsync();

            var expectedStoredProcedure = storedProcedures.Resource.FirstOrDefault(s => s.Id.StartsWith("appendToStream-"));

            var collectionResponse = await collection.ReadContainerAsync();
            var collectionProperties = collectionResponse.Resource;

            var offer = await collection.ReadThroughputAsync(requestOptions: null);

            Assert.That(expectedStoredProcedure, Is.Not.Null);
            Assert.That(offer.Resource.AutoscaleMaxThroughput, Is.EqualTo(collectionThroughput));
            Assert.That(collectionProperties.DefaultTimeToLive, Is.Null);
            Assert.That(collectionProperties.PartitionKeyPath, Is.EqualTo("/streamId"));
            Assert.That(collectionProperties.IndexingPolicy.IncludedPaths.Count, Is.EqualTo(1));
            Assert.That(collectionProperties.IndexingPolicy.IncludedPaths[0].Path, Is.EqualTo("/*"));
            Assert.That(collectionProperties.IndexingPolicy.ExcludedPaths.Count, Is.EqualTo(3));
            Assert.That(collectionProperties.IndexingPolicy.ExcludedPaths[0].Path, Is.EqualTo("/body/*"));
            Assert.That(collectionProperties.IndexingPolicy.ExcludedPaths[1].Path, Is.EqualTo("/metadata/*"));
        }

        [Test]
        public async Task when_using_shared_throughput_with_manual_scale_it_is_set_at_a_database_level()
        {
            const int dbThroughput = 800;
            var collectionName = "SharedCollection_" + Guid.NewGuid();

            await InitialiseStorageEngine(collectionName, dbThroughput: dbThroughput);

            Assert.AreEqual(dbThroughput, await GetDatabaseThroughput(false));
            Assert.AreEqual(null, await GetCollectionThroughput(collectionName, false));
        }

        [Test]
        public async Task when_using_shared_throughput_with_auto_scale_it_is_set_at_a_database_level()
        {
            const int dbThroughput = 8000;
            var collectionName = "SharedCollection_" + Guid.NewGuid();

            await InitialiseStorageEngine(collectionName, dbThroughput: dbThroughput, autoScale: true);

            Assert.AreEqual(dbThroughput, await GetDatabaseThroughput(true));
            Assert.AreEqual(4000, await GetCollectionThroughput(collectionName, true));
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

            Assert.AreEqual(dbThroughput, await GetDatabaseThroughput(false));
            Assert.AreEqual(collectionThroughput, await GetCollectionThroughput(collectionName, false));

            dbThroughput = 1600;
            collectionThroughput = 800;

            await InitialiseStorageEngine(collectionName, collectionThroughput, dbThroughput);

            Assert.AreEqual(dbThroughput, await GetDatabaseThroughput(false));
            Assert.AreEqual(collectionThroughput, await GetCollectionThroughput(collectionName, false));
        }

        [TestCase(null, null, null, 400, false)]
        [TestCase(600, null, 600, null, false)]
        [TestCase(null, 600, null, 600, false)]
        [TestCase(600, 600, 600, 600, false)]
        [TestCase(600, 1000, 600, 1000, false)]
        [TestCase(1000, 600, 1000, 600, false)]

        [TestCase(null, null, 4000, 4000, true)]
        [TestCase(6000, null, 6000, 4000, true)]
        [TestCase(null, 6000, 4000, 6000, true)]
        [TestCase(6000, 6000, 6000, 6000, true)]
        [TestCase(6000, 10000, 6000, 10000, true)]
        [TestCase(10000, 6000, 10000, 6000, true)]
        public async Task set_database_and_collection_throughput_when_database_has_not_been_created(int? dbThroughput, int? collectionThroughput, int? expectedDbThroughput, int? expectedCollectionThroughput, bool autoScale)
        {
            var collectionName = "CollectionThroughput_" + Guid.NewGuid();

            await InitialiseStorageEngine(collectionName, collectionThroughput, dbThroughput, autoScale);

            Assert.AreEqual(expectedDbThroughput, await GetDatabaseThroughput(autoScale));
            Assert.AreEqual(expectedCollectionThroughput, await GetCollectionThroughput(collectionName, autoScale));
        }

        [TestCase(null, 500, null, false)]
        [TestCase(1000, 500, 1000, false)]
        [TestCase(null, 5000, 4000, true)]
        [TestCase(10000, 5000, 10000, true)]
        public async Task set_database_and_collection_throughput_when_database_has_already_been_created(int? collectionThroughput, int expectedDbThroughput, int? expectedCollectionThroughput, bool autoScale)
        {
            await CreateDatabase(expectedDbThroughput, autoScale);
            var collectionName = "CollectionThroughput_" + Guid.NewGuid();

            await InitialiseStorageEngine(collectionName, collectionThroughput, null, autoScale);

            Assert.AreEqual(expectedDbThroughput, await GetDatabaseThroughput(autoScale));
            Assert.AreEqual(expectedCollectionThroughput, await GetCollectionThroughput(collectionName, autoScale));
        }

        private static async Task<IStorageEngine> InitialiseStorageEngine(string collectionName,
            int? collectionThroughput = null,
            int? dbThroughput = null,
            bool autoScale = false)
        {
            var storageEngine = await CosmosDbStorageEngineFactory.Create(collectionName,
                DatabaseName,
                x =>
                {
                    x.UseCollection(o =>
                    {
                        o.CollectionRequestUnits = collectionThroughput;
                        o.UseAutoscale = autoScale;
                    });
                    x.UseDatabase(o =>
                    {
                        o.DatabaseRequestUnits = dbThroughput;
                        o.UseAutoscale = autoScale;
                    });
                });

            return await storageEngine.Initialise();
        }

        public async Task<int?> GetCollectionThroughput(string collectionName, bool autoScale)
        {
            var collection = client.GetContainer(DatabaseName, collectionName);
            if (autoScale)
            {
                var throughputResponse = await collection.ReadThroughputAsync(requestOptions: null);
                return throughputResponse.Resource.AutoscaleMaxThroughput;
            }
            else
            {
                return await collection.ReadThroughputAsync();
            }
        }

        public async Task<int?> GetDatabaseThroughput(bool autoScale)
        {
            var database = client.GetDatabase(DatabaseName);
            if (autoScale)
            {
                var throughputResponse = await database.ReadThroughputAsync(requestOptions: null);
                return throughputResponse.Resource.AutoscaleMaxThroughput;
            }
            else
            {
                return await database.ReadThroughputAsync();
            }
        }

        private Task CreateDatabase(int databaseRequestUnits, bool autoScale)
        {
            if (autoScale)
            {
                return client.CreateDatabaseIfNotExistsAsync(DatabaseName, ThroughputProperties.CreateAutoscaleThroughput(databaseRequestUnits));
            }
            else
            {
                return client.CreateDatabaseIfNotExistsAsync(DatabaseName, ThroughputProperties.CreateManualThroughput(databaseRequestUnits));
            }
        }
    }
}