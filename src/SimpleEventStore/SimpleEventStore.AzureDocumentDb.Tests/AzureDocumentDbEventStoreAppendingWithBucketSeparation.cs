using System.Threading.Tasks;
using SimpleEventStore.Tests;

namespace SimpleEventStore.AzureDocumentDb.Tests
{
    public class AzureDocumentDbEventStoreAppendingWithBucketSeparation : EventStoreAppendingWithBucketSeparation
    {
        protected override Task<IStorageEngine> CreateStorageEngine(string bucket)
        {
            return StorageEngineFactory.Create("AppendingTestsWithBucket", bucket);
        }
    }
}