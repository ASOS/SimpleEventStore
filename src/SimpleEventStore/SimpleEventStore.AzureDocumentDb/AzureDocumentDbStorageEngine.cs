using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Newtonsoft.Json;

namespace SimpleEventStore.AzureDocumentDb
{
    internal class AzureDocumentDbStorageEngine : IStorageEngine
    {
        private readonly DocumentClient client;
        private readonly string databaseName;
        private readonly CollectionOptions collectionOptions;
        private readonly Uri commitsLink;
        private readonly LoggingOptions loggingOptions;
        private readonly ISerializationTypeMap typeMap;
        private readonly JsonSerializer jsonSerializer;
        private readonly DatabaseOptions databaseOptions;
        private readonly Uri databaseUri;
        private Uri storedProcLink;

        internal AzureDocumentDbStorageEngine(DocumentClient client, string databaseName, CollectionOptions collectionOptions, DatabaseOptions databaseOptions, LoggingOptions loggingOptions, ISerializationTypeMap typeMap, JsonSerializer serializer)
        {
            this.client = client;
            this.databaseName = databaseName;
            this.databaseOptions = databaseOptions;
            this.collectionOptions = collectionOptions;
            this.commitsLink = UriFactory.CreateDocumentCollectionUri(databaseName, collectionOptions.CollectionName);
            this.loggingOptions = loggingOptions;
            this.typeMap = typeMap;
            this.jsonSerializer = serializer;
            this.databaseUri = UriFactory.CreateDatabaseUri(databaseName);
        }

        public async Task<IStorageEngine> Initialise(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await CreateDatabaseIfItDoesNotExist();

            cancellationToken.ThrowIfCancellationRequested();
            await CreateCollectionIfItDoesNotExist();
            
            cancellationToken.ThrowIfCancellationRequested();
            await Task.WhenAll(
                InitialiseStoredProcedure(),
                SetDatabaseOfferThroughput(),
                SetCollectionOfferThroughput()
                );

            return this;
        }

        public async Task AppendToStream(string streamId, IEnumerable<StorageEvent> events, CancellationToken cancellationToken = default)
        {
            var docs = events.Select(d => DocumentDbStorageEvent.FromStorageEvent(d, this.typeMap, this.jsonSerializer)).ToList();

            try
            {
                var result = await this.client.ExecuteStoredProcedureAsync<dynamic>(
                    storedProcLink,
                    new RequestOptions { PartitionKey = new PartitionKey(streamId), ConsistencyLevel = this.collectionOptions.ConsistencyLevel },
                    cancellationToken,
                    docs);

                loggingOptions.OnSuccess(ResponseInformation.FromWriteResponse(nameof(AppendToStream), result));
            }
            catch (DocumentClientException ex) when (ex.ResponseHeaders["x-ms-substatus"] == "409")
            {
                throw new ConcurrencyException(ex.Error.Message, ex);
            }
        }

        public async Task<IReadOnlyCollection<StorageEvent>> ReadStreamForwards(string streamId, int startPosition, int numberOfEventsToRead, CancellationToken cancellationToken = default)
        {
            int endPosition = numberOfEventsToRead == int.MaxValue ? int.MaxValue : startPosition + numberOfEventsToRead;

            var eventsQuery = this.client.CreateDocumentQuery<DocumentDbStorageEvent>(commitsLink)
                .Where(x => x.StreamId == streamId && x.EventNumber >= startPosition && x.EventNumber <= endPosition)
                .OrderBy(x => x.EventNumber)
                .AsDocumentQuery();

            var events = new List<StorageEvent>();

            while (eventsQuery.HasMoreResults)
            {
                var response = await eventsQuery.ExecuteNextAsync<DocumentDbStorageEvent>(cancellationToken);
                loggingOptions.OnSuccess(ResponseInformation.FromReadResponse(nameof(ReadStreamForwards), response));

                foreach (var e in response)
                {
                    events.Add(e.ToStorageEvent(this.typeMap, this.jsonSerializer));
                }
            }

            return events.AsReadOnly();
        }

        private Task CreateDatabaseIfItDoesNotExist()
        {
            return client.CreateDatabaseIfNotExistsAsync(
                new Database { Id = databaseName },
                new RequestOptions
                {
                    OfferThroughput = databaseOptions.DatabaseRequestUnits
                });
        }

        private Task CreateCollectionIfItDoesNotExist()
        {
            var collection = new DocumentCollection
            {
                Id = collectionOptions.CollectionName,
                DefaultTimeToLive = collectionOptions.DefaultTimeToLive
            };

            collection.PartitionKey.Paths.Add("/streamId");
            collection.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/*" });
            collection.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/body/*" });
            collection.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/metadata/*" });

            var requestOptions = new RequestOptions
            {
                OfferThroughput = collectionOptions.CollectionRequestUnits
            };

            return client.CreateDocumentCollectionIfNotExistsAsync(databaseUri, collection, requestOptions);
        }

        private async Task InitialiseStoredProcedure()
        {
            var sproc = AppendSprocProvider.GetAppendSprocData();
            storedProcLink = UriFactory.CreateStoredProcedureUri(databaseName, collectionOptions.CollectionName, sproc.Name);

            var query = client.CreateStoredProcedureQuery(commitsLink)
                .Where(x => x.Id == sproc.Name)
                .AsDocumentQuery();

            if (!(await query.ExecuteNextAsync<StoredProcedure>()).Any())
            {
                await client.CreateStoredProcedureAsync(commitsLink, new StoredProcedure
                {
                    Id = sproc.Name,
                    Body = sproc.Body
                });
            }
        }
        private async Task SetCollectionOfferThroughput()
        {
            if (collectionOptions.CollectionRequestUnits != null)
            {
                var collection =
                    (await client.ReadDocumentCollectionAsync(
                        UriFactory.CreateDocumentCollectionUri(databaseName, collectionOptions.CollectionName)))
                    .Resource;

                await SetOfferThroughput(collection.SelfLink, (int)collectionOptions.CollectionRequestUnits);
            }
        }

        private async Task SetDatabaseOfferThroughput()
        {
            if (databaseOptions.DatabaseRequestUnits != null)
            {
                var db = (await client.ReadDatabaseAsync(databaseUri)).Resource;

                await SetOfferThroughput(db.SelfLink, (int)databaseOptions.DatabaseRequestUnits);
            }
        }

        private Task SetOfferThroughput(string resourceLink, int throughput)
        {
            var offer = client
                .CreateOfferQuery()
                .Where(x => x.ResourceLink == resourceLink)
                .AsEnumerable()
                .FirstOrDefault();

            return client.ReplaceOfferAsync(new OfferV2(offer, throughput));
        }
    }
}
