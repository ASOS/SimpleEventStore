using NUnit.Framework;
using System.Threading.Tasks;

namespace SimpleEventStore.Tests
{
    public abstract class EventStoreTestBase
    {
        protected EventStore Subject { get; private set; }

        [OneTimeSetUp]
        public async Task OneTimeSetUp()
        {
            var storageEngine = await CreateStorageEngine();
            Subject = new EventStore(storageEngine);
        }


        protected abstract Task<IStorageEngine> CreateStorageEngine();
    }
}