using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using Microsoft.Azure.Cosmos.Scripts;
using Newtonsoft.Json;

namespace SimpleEventStore.CosmosDb
{
    internal class AzureCosmosDbStorageEngine : IStorageEngine
    {
        private readonly CosmosClient _client;
        private readonly string _databaseName;
        private readonly CollectionOptions collectionOptions;
        private readonly LoggingOptions _loggingOptions;
        private readonly ISerializationTypeMap _typeMap;
        private readonly JsonSerializer _jsonSerializer;
        private readonly DatabaseOptions _databaseOptions;
        private Database _database;
        private Container _collection;
        private (string Name, string Body) _storedProcedureInformation;

        internal AzureCosmosDbStorageEngine(CosmosClient client, string databaseName,
            CollectionOptions collectionOptions, DatabaseOptions databaseOptions, LoggingOptions loggingOptions,
            ISerializationTypeMap typeMap, JsonSerializer serializer)
        {
            _client = client;
            _databaseName = databaseName;
            _databaseOptions = databaseOptions;
            this.collectionOptions = collectionOptions;
            _loggingOptions = loggingOptions;
            _typeMap = typeMap;
            _jsonSerializer = serializer;
        }

        public async Task<IStorageEngine> Initialise(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var databaseResponse = await CreateDatabaseIfItDoesNotExist();
            _database = databaseResponse.Database;

            cancellationToken.ThrowIfCancellationRequested();
            var containerResponse = (await CreateCollectionIfItDoesNotExist());
            _collection = containerResponse.Container;

            cancellationToken.ThrowIfCancellationRequested();
            await Task.WhenAll(
                InitialiseStoredProcedure(),
                SetDatabaseOfferThroughput(),
                SetCollectionOfferThroughput()
            );

            return this;
        }

        public async Task AppendToStream(string streamId, IEnumerable<StorageEvent> events,
            CancellationToken cancellationToken = default)
        {
            var docs = events.Select(d => CosmosDbStorageEvent.FromStorageEvent(d, _typeMap, _jsonSerializer))
                .ToList();

            try
            {
                var result = await _collection.Scripts.ExecuteStoredProcedureAsync<dynamic>(
                    _storedProcedureInformation.Name,
                    new PartitionKey(streamId),
                    new[] {docs},
                    new StoredProcedureRequestOptions
                    {
                        ConsistencyLevel = collectionOptions.ConsistencyLevel
                    },
                    cancellationToken);

                _loggingOptions.OnSuccess(ResponseInformation.FromWriteResponse(nameof(AppendToStream), result));
            }
            catch (CosmosException ex) when (ex.Headers["x-ms-substatus"] == "409" || ex.SubStatusCode == 409)
            {
                throw new ConcurrencyException(ex.ResponseBody, ex);
            }
        }

        public async Task<IReadOnlyCollection<StorageEvent>> ReadStreamForwards(string streamId, int startPosition,
            int numberOfEventsToRead, CancellationToken cancellationToken = default)
        {
            int endPosition = numberOfEventsToRead == int.MaxValue
                ? int.MaxValue
                : startPosition + numberOfEventsToRead - 1;

            var queryDefinition = new QueryDefinition(@"
                    SELECT VALUE e
                    FROM e
                    WHERE e.streamId = @StreamId
                        AND (e.eventNumber BETWEEN @LowerBound AND @UpperBound)
                    ORDER BY e.eventNumber ASC"
                )
                .WithParameter("@StreamId", streamId)
                .WithParameter("@LowerBound", startPosition)
                .WithParameter("@UpperBound", endPosition);

            var options = new QueryRequestOptions
            {
                MaxItemCount = numberOfEventsToRead,
                PartitionKey = new PartitionKey(streamId)
            };

            using var eventsQuery = _collection.GetItemQueryIterator<CosmosDbStorageEvent>(queryDefinition, requestOptions: options);
            var events = new List<StorageEvent>();

            while (eventsQuery.HasMoreResults)
            {
                var response = await eventsQuery.ReadNextAsync(cancellationToken);
                _loggingOptions.OnSuccess(ResponseInformation.FromReadResponse(nameof(ReadStreamForwards), response));

                foreach (var e in response)
                {
                    events.Add(e.ToStorageEvent(_typeMap, _jsonSerializer));
                }
            }

            return events.AsReadOnly();
        }

        private Task<DatabaseResponse> CreateDatabaseIfItDoesNotExist()
        {
            return _client.CreateDatabaseIfNotExistsAsync(_databaseName, _databaseOptions.DatabaseRequestUnits);
        }

        private Task<ContainerResponse> CreateCollectionIfItDoesNotExist()
        {
            var collectionProperties = new ContainerProperties()
            {
                Id = collectionOptions.CollectionName,
                IndexingPolicy = new IndexingPolicy
                {
                    IncludedPaths =
                    {
                        new IncludedPath {Path = "/*"},
                    },
                    ExcludedPaths =
                    {
                        new ExcludedPath {Path = "/body/*"},
                        new ExcludedPath {Path = "/metadata/*"}
                    }
                },
                DefaultTimeToLive = collectionOptions.DefaultTimeToLive,
                PartitionKeyPath = "/streamId"
            };

            return _database.CreateContainerIfNotExistsAsync(collectionProperties,
                collectionOptions.CollectionRequestUnits);
        }

        private async Task InitialiseStoredProcedure()
        {
            _storedProcedureInformation = AppendSprocProvider.GetAppendSprocData();
            var storedProcedures = await _collection.Scripts.GetStoredProcedureQueryIterator<StoredProcedureProperties>(
                $"SELECT * FROM s where s.id = '{_storedProcedureInformation.Name}'").ReadNextAsync();

            if (!storedProcedures.Resource.Any())
            {
                await _collection.Scripts.CreateStoredProcedureAsync(
                    new StoredProcedureProperties(_storedProcedureInformation.Name, _storedProcedureInformation.Body));
            }
        }

        private async Task SetCollectionOfferThroughput()
        {
            if (collectionOptions.CollectionRequestUnits != null)
            {
                await _collection.ReplaceThroughputAsync((int) collectionOptions.CollectionRequestUnits);
            }
        }

        private async Task SetDatabaseOfferThroughput()
        {
            if (_databaseOptions.DatabaseRequestUnits != null)
            {
                await _database.ReplaceThroughputAsync((int) _databaseOptions.DatabaseRequestUnits);
            }
        }
    }
}