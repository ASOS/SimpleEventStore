using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleEventStore.InMemory
{
    public class InMemoryStorageEngine : IStorageEngine
    {
        private readonly ConcurrentDictionary<string, List<StorageEvent>> streams = new ConcurrentDictionary<string, List<StorageEvent>>();
        private readonly List<StorageEvent> allEvents = new List<StorageEvent>();
        private readonly List<Subscription> subscriptions = new List<Subscription>();

        public Task AppendToStream(string streamId, IEnumerable<StorageEvent> events)
        {
            return Task.Run(() =>
            {
                if (!streams.ContainsKey(streamId))
                {
                    streams[streamId] = new List<StorageEvent>();
                }

                var firstEvent = events.First();

                lock (streams[streamId])
                {
                    if (firstEvent.EventNumber - 1 != streams[streamId].Count)
                    {
                        throw new ConcurrencyException($"Concurrency conflict when appending to stream {@streamId}. Expected revision {firstEvent.EventNumber} : Actual revision {streams[streamId].Count}");
                    }

                    streams[streamId].AddRange(events);
                }

                AddEventsToAllStream(events);
            });
        }

        private void AddEventsToAllStream(IEnumerable<StorageEvent> events)
        {
            lock (allEvents)
            {
                foreach (var e in events)
                {
                    allEvents.Add(e);
                }
            }
        }

        public Task<IReadOnlyCollection<StorageEvent>> ReadStreamForwards(string streamId, int startPosition, int numberOfEventsToRead)
        {
            IReadOnlyCollection<StorageEvent> result = streams[streamId].Skip(startPosition - 1).Take(numberOfEventsToRead).ToList().AsReadOnly();
            return Task.FromResult(result);
        }

        public void SubscribeToAll(EventsReceivedCallback onNextEvent, string checkpoint, CancellationToken cancellationToken)
        {
            Guard.IsNotNull(nameof(onNextEvent), onNextEvent);

            var subscription = new Subscription(this.allEvents, onNextEvent, checkpoint);
            this.subscriptions.Add(subscription);
            subscription.Start(cancellationToken);
        }

        public async Task ReadAllForwards(EventsReceivedCallback onNextEvent, string sinceCheckpoint)
        {
            Guard.IsNotNull(nameof(onNextEvent), onNextEvent);

            var subscription = new Subscription(allEvents, onNextEvent, sinceCheckpoint);
            await subscription.ReadEvents();
        }

        private class Subscription
        {
            private readonly IEnumerable<StorageEvent> allStream;
            private readonly EventsReceivedCallback onNewEvent;
            private string doNotDispatchUntilCheckpointEncountered;
            private int currentPosition;
            private Task workerTask;

            public Subscription(IEnumerable<StorageEvent> allStream, EventsReceivedCallback onNewEvent, string checkpoint)
            {
                this.allStream = allStream;
                this.onNewEvent = onNewEvent;
                this.doNotDispatchUntilCheckpointEncountered = checkpoint;
            }

            public void Start(CancellationToken cancellationToken)
            {
                workerTask = Task.Run(async () =>
                {
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        await ReadEvents();
                        await Task.Delay(500);
                    }
                });
            }

            public async Task ReadEvents()
            {
                List<StorageEvent> snapshot;

                lock (allStream)
                {
                    snapshot = allStream.Skip(this.currentPosition).ToList();
                }

                // Note that callback is only invoked if there are new events in the stream.
                // If that implementation changes (e.g. to invoke on every poll) then might be best to
                // ensure a consistent testing experience by updating AzureDocumentDbStorageEngine too.

                bool dispatchEvents = this.doNotDispatchUntilCheckpointEncountered == null;
                foreach (var @event in snapshot)
                {
                    if (!dispatchEvents && this.doNotDispatchUntilCheckpointEncountered == @event.EventId.ToString())
                    {
                        this.doNotDispatchUntilCheckpointEncountered = null;
                        dispatchEvents = true;
                        continue;
                    }

                    if(dispatchEvents)
                    {
                        await this.onNewEvent(new[] { @event }, @event.EventId.ToString());
                        this.currentPosition++;
                    }
                }
            }
        }
    }
}