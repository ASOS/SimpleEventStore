using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace SimpleEventStore
{
    public interface IStorageEngine
    {
        Task AppendToStream(string streamId, IEnumerable<StorageEvent> events);

        Task<IReadOnlyCollection<StorageEvent>> ReadStreamForwards(string streamId, int startPosition, int numberOfEventsToRead);

        Task<IStorageEngine> Initialise();
    }
}