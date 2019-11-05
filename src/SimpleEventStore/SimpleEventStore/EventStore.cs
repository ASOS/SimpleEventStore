using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleEventStore
{
    public class EventStore
    {
        private readonly IStorageEngine engine;

        public EventStore(IStorageEngine engine)
        {
            this.engine = engine;
        }

        public Task AppendToStream(string streamId, int expectedVersion, params EventData[] events)
        {
            return AppendToStream(streamId, expectedVersion, default, events);
        }

        public Task AppendToStream(string streamId, int expectedVersion, CancellationToken cancellationToken, params EventData[] events)
        {
            Guard.IsNotNullOrEmpty(nameof(streamId), streamId);

            var storageEvents = new List<StorageEvent>();
            var eventVersion = expectedVersion;

            for (int i = 0; i < events.Length; i++)
            {
                storageEvents.Add(new StorageEvent(streamId, events[i], ++eventVersion));
            }

            return engine.AppendToStream(streamId, storageEvents, cancellationToken);
        }

        public Task<IReadOnlyCollection<StorageEvent>> ReadStreamForwards(string streamId, CancellationToken cancellationToken = default)
        {
            Guard.IsNotNullOrEmpty(nameof(streamId), streamId);

            return engine.ReadStreamForwards(streamId, 1, Int32.MaxValue, cancellationToken);
        }

        public Task<IReadOnlyCollection<StorageEvent>> ReadStreamForwards(string streamId, int startPosition, int numberOfEventsToRead, CancellationToken cancellationToken = default)
        {
            Guard.IsNotNullOrEmpty(nameof(streamId), streamId);

            return engine.ReadStreamForwards(streamId, startPosition, numberOfEventsToRead, cancellationToken);
        }
    }
}