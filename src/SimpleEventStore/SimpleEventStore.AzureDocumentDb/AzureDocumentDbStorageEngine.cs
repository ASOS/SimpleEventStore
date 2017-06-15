using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using System.Threading;

namespace SimpleEventStore.AzureDocumentDb
{
    public class AzureDocumentDbStorageEngine : IStorageEngine
    {
        private const string CommitsCollectionName = "Commits";
        private const string AppendStoredProcedureName = "appendToStream";
        private const string ConcurrencyConflictErrorKey = "Concurrency conflict.";

        private readonly DocumentClient client;
        private readonly string databaseName;
        private readonly DatabaseOptions databaseOptions;
        private readonly Uri commitsLink;
        private readonly Uri storedProcLink;
        private readonly List<Subscription> subscriptions = new List<Subscription>();
        private readonly SubscriptionOptions subscriptionOptions;

        public AzureDocumentDbStorageEngine(DocumentClient client, string databaseName, DatabaseOptions databaseOptions, SubscriptionOptions subscriptionOptions)
        {
            this.client = client;
            this.databaseName = databaseName;
            this.databaseOptions = databaseOptions;
            this.commitsLink = UriFactory.CreateDocumentCollectionUri(databaseName, CommitsCollectionName);
            this.storedProcLink = UriFactory.CreateStoredProcedureUri(databaseName, CommitsCollectionName, AppendStoredProcedureName);
            this.subscriptionOptions = subscriptionOptions;
        }

        public async Task Initialise()
        {
            await CreateDatabaseIfItDoesNotExist();
            await CreateCollectionIfItDoesNotExist();
            await CreateAppendStoredProcedureIfItDoesNotExist();
        }

        public async Task AppendToStream(string streamId, IEnumerable<StorageEvent> events)
        {
            var docs = events.Select(d => DocumentDbStorageEvent.FromStorageEvent(d)).ToList();

            try
            {
                var result = await this.client.ExecuteStoredProcedureAsync<dynamic>(
                    storedProcLink, 
                    new RequestOptions { PartitionKey = new PartitionKey(streamId), ConsistencyLevel = this.databaseOptions.ConsistencyLevel },
                    docs);
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

            var eventsQuery = this.client.CreateDocumentQuery<DocumentDbStorageEvent>(commitsLink)
                .Where(x => x.StreamId == streamId && x.EventNumber >= startPosition && x.EventNumber <= endPosition)
                .OrderBy(x => x.EventNumber)
                .AsDocumentQuery();

            var events = new List<StorageEvent>();

            while (eventsQuery.HasMoreResults)
            {
                foreach (var e in await eventsQuery.ExecuteNextAsync<DocumentDbStorageEvent>())
                {
                    events.Add(e.ToStorageEvent());
                }
            }

            return events.AsReadOnly();
        }

        public void SubscribeToAll(Action<IReadOnlyCollection<StorageEvent>, string> onNextEvent, string checkpoint, CancellationToken cancellationToken)
        {
            Guard.IsNotNull(nameof(onNextEvent), onNextEvent);

            var subscription = new Subscription(this.client, this.commitsLink, onNextEvent, checkpoint, this.subscriptionOptions);
            subscriptions.Add(subscription);

            subscription.Start(cancellationToken);
        }

        private async Task CreateDatabaseIfItDoesNotExist()
        {
            var databaseExistsQuery = client.CreateDatabaseQuery()
                .Where(x => x.Id == databaseName)
                .Take(1)
                .AsDocumentQuery();

            if (!(await databaseExistsQuery.ExecuteNextAsync<Database>()).Any())
            {
                await client.CreateDatabaseAsync(new Database {Id = databaseName});
            }
        }

        private async Task CreateCollectionIfItDoesNotExist()
        {
            var databaseUri = UriFactory.CreateDatabaseUri(databaseName);

            var commitsCollectionQuery = client.CreateDocumentCollectionQuery(databaseUri)
                .Where(x => x.Id == CommitsCollectionName)
                .Take(1)
                .AsDocumentQuery();

            if (!(await commitsCollectionQuery.ExecuteNextAsync<DocumentCollection>()).Any())
            {
                var collection = new DocumentCollection();
                collection.Id = CommitsCollectionName;
                collection.PartitionKey.Paths.Add("/streamId");
                collection.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/*" });
                collection.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/body/*"});
                collection.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/metadata/*" });

                var requestOptions = new RequestOptions
                {
                    OfferThroughput = this.databaseOptions.CollectionRequestUnits
                };

                await client.CreateDocumentCollectionAsync(databaseUri, collection, requestOptions);
            }
        }

        private async Task CreateAppendStoredProcedureIfItDoesNotExist()
        {
            var query = client.CreateStoredProcedureQuery(commitsLink)
                .Where(x => x.Id == AppendStoredProcedureName)
                .AsDocumentQuery();

            if (!(await query.ExecuteNextAsync<StoredProcedure>()).Any())
            { 
                await client.CreateStoredProcedureAsync(commitsLink, new StoredProcedure
                {
                    Id = AppendStoredProcedureName,
                    Body = Resources.GetString("AppendToStream.js")
                });
            }
        }
    }
}
