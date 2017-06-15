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

        public void SubscribeToAll(Action<IReadOnlyCollection<StorageEvent>, string> onNextEvent, string checkpoint, CancellationToken cancellationToken)
        {
            Guard.IsNotNull(nameof(onNextEvent), onNextEvent);

            var subscription = new Subscription(this.allEvents, onNextEvent, checkpoint);
            this.subscriptions.Add(subscription);
            subscription.Start(cancellationToken);
        }

        private class Subscription
        {
            private readonly IEnumerable<StorageEvent> allStream;
            private readonly Action<IReadOnlyCollection<StorageEvent>, string> onNewEvent;
            private string initialCheckpoint;
            private int currentPosition;
            private Task workerTask;

            public Subscription(IEnumerable<StorageEvent> allStream, Action<IReadOnlyCollection<StorageEvent>, string> onNewEvent, string checkpoint)
            {
                this.allStream = allStream;
                this.onNewEvent = onNewEvent;
                this.initialCheckpoint = checkpoint;
            }

            public void Start(CancellationToken cancellationToken)
            {
                workerTask = Task.Run(async () =>
                {
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        ReadEvents();
                        await Task.Delay(500);
                    }
                });
            }

            private void ReadEvents()
            {
                List<StorageEvent> snapshot;

                lock (allStream)
                {
                    snapshot = allStream.Skip(this.currentPosition).ToList();
                }

                foreach (var @event in snapshot)
                {
                    bool dispatchEvents = true;

                    if (this.initialCheckpoint == null || this.initialCheckpoint == @event.EventId.ToString())
                    {
                        dispatchEvents = this.initialCheckpoint == null;
                    }

                    if(dispatchEvents)
                    {
                        this.onNewEvent(new[] { @event }, @event.EventId.ToString());
                        this.currentPosition++;
                    }
                }
            }
        }
    }
}