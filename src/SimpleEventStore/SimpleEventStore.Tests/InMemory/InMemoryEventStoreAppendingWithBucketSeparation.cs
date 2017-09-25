using System.Threading.Tasks;
using SimpleEventStore.InMemory;

namespace SimpleEventStore.Tests.InMemory
{
    public class InMemoryEventStoreAppendingWithBucketSeparation : EventStoreAppendingWithBucketSeparation
    {
        protected override Task<IStorageEngine> CreateStorageEngine(string bucket)
        {
            return Task.FromResult((IStorageEngine)new InMemoryStorageEngine());
        }
    }
}