using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleEventStore
{
    public delegate Task EventsReceivedCallback(IReadOnlyCollection<StorageEvent> events, string newCheckpoint);

    public interface IStorageEngine
    {
        Task AppendToStream(string streamId, IEnumerable<StorageEvent> events);

        Task<IReadOnlyCollection<StorageEvent>> ReadStreamForwards(string streamId, int startPosition, int numberOfEventsToRead);

        void SubscribeToAll(EventsReceivedCallback onNextEvent, string checkpoint, CancellationToken cancellationToken);

        Task ReadAllForwards(EventsReceivedCallback onNextEvent, string sinceCheckpoint);
    }
}