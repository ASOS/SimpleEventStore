using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleEventStore
{
    public interface IStorageEngine
    {
        Task AppendToStream(string streamId, IEnumerable<StorageEvent> events, CancellationToken cancellationToken = default);

        Task<IReadOnlyCollection<StorageEvent>> ReadStreamForwards(string streamId, int startPosition, int numberOfEventsToRead, CancellationToken cancellationToken = default);

        Task<IStorageEngine> Initialise(CancellationToken cancellationToken = default);
    }
}