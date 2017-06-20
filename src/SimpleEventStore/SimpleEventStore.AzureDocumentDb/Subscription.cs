using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Newtonsoft.Json;
using System.Threading;

namespace SimpleEventStore.AzureDocumentDb
{
    internal class Subscription
    {
        private readonly DocumentClient client;
        private readonly Uri commitsLink;
        private readonly EventsReceivedCallback onNextEvent;
        private readonly SubscriptionOptions subscriptionOptions;
        private readonly Dictionary<string, string> checkpoints;
        private Task workerTask;

        public Subscription(DocumentClient client, Uri commitsLink, EventsReceivedCallback onNextEvent, string checkpoint, SubscriptionOptions subscriptionOptions)
        {
            this.client = client;
            this.commitsLink = commitsLink;
            this.onNextEvent = onNextEvent;
            this.checkpoints = checkpoint == null ? new Dictionary<string, string>() : JsonConvert.DeserializeObject<Dictionary<string, string>>(checkpoint);
            this.subscriptionOptions = subscriptionOptions;
        }

        // TODO: Configure the retry policy
        public void Start(CancellationToken cancellationToken)
        {
            workerTask = Task.Run(async () =>
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    await ReadEvents();
                    await Task.Delay(subscriptionOptions.PollEvery);
                }
            });
        }

        public async Task ReadEvents()
        {
            var partitionKeyRanges = new List<PartitionKeyRange>();
            FeedResponse<PartitionKeyRange> pkRangesResponse;

            do
            {
                pkRangesResponse = await client.ReadPartitionKeyRangeFeedAsync(commitsLink);
                partitionKeyRanges.AddRange(pkRangesResponse);
            }
            while (pkRangesResponse.ResponseContinuation != null);

            foreach (var pkRange in partitionKeyRanges)
            {
                string continuation;
                checkpoints.TryGetValue(pkRange.Id, out continuation);

                IDocumentQuery<Document> query = client.CreateDocumentChangeFeedQuery(
                    commitsLink,
                    new ChangeFeedOptions
                    {
                        PartitionKeyRangeId = pkRange.Id,
                        StartFromBeginning = true,
                        RequestContinuation = continuation,
                        MaxItemCount = subscriptionOptions.MaxItemCount
                    });

                while (query.HasMoreResults)
                {
                    var feedResponse = await query.ExecuteNextAsync<Document>();
                    var events = new List<StorageEvent>();
                    string initialCheckpointValue;

                    foreach (var @event in feedResponse)
                    {
                        events.Add(DocumentDbStorageEvent.FromDocument(@event).ToStorageEvent());
                    }

                    checkpoints.TryGetValue(pkRange.Id, out initialCheckpointValue);

                    try
                    {
                        checkpoints[pkRange.Id] = feedResponse.ResponseContinuation;

                        if (events.Count > 0)
                        {
                            // InMemoryStorageEngine only invokes callback if events were found in the feed.
                            // This matches the idea of a change-feed that is not coupled to an implementation (i.e. a polling client)
                            // To optimise writes to the checkpoint token store and to make for a consistent
                            // testing experience across all stores, DocDB implementation matches InMemory implementation.
                            // If this is changed, probably best to change InMemory implementation too.
                            await this.onNextEvent(events.AsReadOnly(), JsonConvert.SerializeObject(checkpoints));
                        }
                    }
                    catch(Exception)
                    {
                        if (initialCheckpointValue != null)
                        {
                            checkpoints[pkRange.Id] = initialCheckpointValue;
                        }
                        throw;
                    }
                }
            }
        }
    }
}