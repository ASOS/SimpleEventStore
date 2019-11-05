using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleEventStore.InMemory
{
    public class InMemoryStorageEngine : IStorageEngine
    {
        private static readonly IReadOnlyCollection<StorageEvent> EmptyStream = new StorageEvent[0];

        private readonly ConcurrentDictionary<string, List<StorageEvent>> streams = new ConcurrentDictionary<string, List<StorageEvent>>();
        private readonly List<StorageEvent> allEvents = new List<StorageEvent>();

        public Task AppendToStream(string streamId, IEnumerable<StorageEvent> events, CancellationToken cancellationToken = default)
        {
            return Task.Run(() =>
            {
                if (!streams.ContainsKey(streamId))
                {
                    streams[streamId] = new List<StorageEvent>();
                }

                var firstEvent = events.First();

                if (firstEvent.EventNumber - 1 != streams[streamId].Count)
                {
                    throw new ConcurrencyException($"Concurrency conflict when appending to stream {streamId}. Expected revision {firstEvent.EventNumber} : Actual revision {streams[streamId].Count}");
                }

                cancellationToken.ThrowIfCancellationRequested();

                streams[streamId].AddRange(events);
                AddEventsToAllStream(events);
            });
        }

        private void AddEventsToAllStream(IEnumerable<StorageEvent> events)
        {
            foreach (var e in events)
            {
                allEvents.Add(e);
            }
        }

        public Task<IReadOnlyCollection<StorageEvent>> ReadStreamForwards(string streamId, int startPosition, int numberOfEventsToRead, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!streams.ContainsKey(streamId))
            {
                return Task.FromResult(EmptyStream);
            }

            IReadOnlyCollection<StorageEvent> stream = streams[streamId].Skip(startPosition - 1).Take(numberOfEventsToRead).ToList().AsReadOnly();
            return Task.FromResult(stream);
        }

        public Task<IStorageEngine> Initialise(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            return Task.FromResult<IStorageEngine>(this);
        }
    }
}