using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;

namespace SimpleEventStore.AzureDocumentDb
{
    public class AzureDocumentDbStorageEngine : IStorageEngine
    {
        private const string AppendStoredProcedureName = "appendToStream";
        private const string ConcurrencyConflictErrorKey = "Concurrency conflict.";

        private readonly string databaseName;
        private readonly CollectionOptions collectionOptions;
        private readonly Uri storedProcLink;
        private readonly LoggingOptions loggingOptions;

        public const string DefaultBucket = "default";

        internal AzureDocumentDbStorageEngine(DocumentClient client, string databaseName, CollectionOptions collectionOptions, LoggingOptions loggingOptions, ISerializationTypeMap typeMap)
        {
            this.Client = client;
            this.databaseName = databaseName;
            this.collectionOptions = collectionOptions;
            this.CommitsLink = UriFactory.CreateDocumentCollectionUri(databaseName, collectionOptions.CollectionName);
            this.storedProcLink = UriFactory.CreateStoredProcedureUri(databaseName, collectionOptions.CollectionName, AppendStoredProcedureName);
            this.loggingOptions = loggingOptions;
            this.TypeMap = typeMap;
        }

        public Uri CommitsLink { get; }

        public ISerializationTypeMap TypeMap { get; }

        public DocumentClient Client { get; }

        public async Task<IStorageEngine> Initialise()
        {
            await CreateDatabaseIfItDoesNotExist();
            await CreateCollectionIfItDoesNotExist();
            await CreateAppendStoredProcedureIfItDoesNotExist();

            return this;
        }

        public async Task AppendToStream(string streamId, IEnumerable<StorageEvent> events)
        {
            var docs = events.Select(d => DocumentDbStorageEvent.FromStorageEvent(d, this.TypeMap, collectionOptions.Bucket)).ToList();

            try
            {
                var result = await this.Client.ExecuteStoredProcedureAsync<dynamic>(
                    storedProcLink, 
                    new RequestOptions { PartitionKey = new PartitionKey(streamId), ConsistencyLevel = this.collectionOptions.ConsistencyLevel },
                    docs);

                loggingOptions.OnSuccess(ResponseInformation.FromWriteResponse(result));
            }
            catch (DocumentClientException ex)
            {
                if (ex.Error.Message.Contains(ConcurrencyConflictErrorKey))
                {
                    throw new ConcurrencyException(ex.Error.Message, ex);
                }

                throw;
            }
        }

        public async Task<IReadOnlyCollection<StorageEvent>> ReadStreamForwards(string streamId, int startPosition, int numberOfEventsToRead)
        {
            int endPosition = numberOfEventsToRead == int.MaxValue ? int.MaxValue : startPosition + numberOfEventsToRead;

            var eventsQuery = this.Client.CreateDocumentQuery<DocumentDbStorageEvent>(CommitsLink)
                .Where(x => x.StreamId == streamId && (x.Bucket ?? DefaultBucket) == collectionOptions.Bucket && x.EventNumber >= startPosition && x.EventNumber <= endPosition)
                .OrderBy(x => x.EventNumber)
                .AsDocumentQuery();

            var events = new List<StorageEvent>();

            while (eventsQuery.HasMoreResults)
            {
                var response = await eventsQuery.ExecuteNextAsync<DocumentDbStorageEvent>();
                loggingOptions.OnSuccess(ResponseInformation.FromReadResponse(response));

                foreach (var e in response)
                {
                    events.Add(e.ToStorageEvent(this.TypeMap));
                }
            }

            return events.AsReadOnly();
        }

        private async Task CreateDatabaseIfItDoesNotExist()
        {
            var databaseExistsQuery = Client.CreateDatabaseQuery()
                .Where(x => x.Id == databaseName)
                .Take(1)
                .AsDocumentQuery();

            if (!(await databaseExistsQuery.ExecuteNextAsync<Database>()).Any())
            {
                await Client.CreateDatabaseAsync(new Database {Id = databaseName});
            }
        }

        private async Task CreateCollectionIfItDoesNotExist()
        {
            var databaseUri = UriFactory.CreateDatabaseUri(databaseName);

            var commitsCollectionQuery = Client.CreateDocumentCollectionQuery(databaseUri)
                .Where(x => x.Id == collectionOptions.CollectionName)
                .Take(1)
                .AsDocumentQuery();

            if (!(await commitsCollectionQuery.ExecuteNextAsync<DocumentCollection>()).Any())
            {
                var collection = new DocumentCollection();
                collection.Id = collectionOptions.CollectionName;
                collection.PartitionKey.Paths.Add("/streamId");
                collection.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/*" });
                collection.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/body/*"});
                collection.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/metadata/*" });

                var requestOptions = new RequestOptions
                {
                    OfferThroughput = collectionOptions.CollectionRequestUnits
                };

                await Client.CreateDocumentCollectionAsync(databaseUri, collection, requestOptions);
            }
        }

        private async Task CreateAppendStoredProcedureIfItDoesNotExist()
        {
            var query = Client.CreateStoredProcedureQuery(CommitsLink)
                .Where(x => x.Id == AppendStoredProcedureName)
                .AsDocumentQuery();

            var sprc = await query.ExecuteNextAsync<StoredProcedure>();
            var appendToStreamSproc = Resources.GetString("AppendToStream.js");
            if (sprc.Count > 0)
            {
                var tmp = sprc.FirstOrDefault();
                if (tmp != null && tmp.Body != appendToStreamSproc)
                {
                    //  can't use replacesproc here because it's not supported on partitioned collections
                    await Client.DeleteStoredProcedureAsync(tmp.SelfLink);
                }
            }

            if (!(await query.ExecuteNextAsync<StoredProcedure>()).Any())
            { 
                await Client.CreateStoredProcedureAsync(CommitsLink, new StoredProcedure
                {
                    Id = AppendStoredProcedureName,
                    Body = appendToStreamSproc
                });
            }
        }
    }
}
