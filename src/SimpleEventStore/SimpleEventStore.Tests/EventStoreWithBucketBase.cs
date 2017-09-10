using System.Threading.Tasks;

namespace SimpleEventStore.Tests
{
    public abstract class EventStoreWithBucketBase
    {
        protected const string DefaultBucket = "default";
        protected const string OrdersBucket = "Orders";
        protected const string PaymentsBucket = "Payments";

        protected async Task<EventStore> GetEventStoreWithNoBucketSpecified()
        {
            var storageEngine = await CreateStorageEngine(DefaultBucket);
            return new EventStore(storageEngine);
        }

        protected async Task<EventStore> GetOrdersEventStore()
        {
            var storageEngine = await CreateStorageEngine(OrdersBucket);
            return new EventStore(storageEngine);
        }

        protected async Task<EventStore> GetPaymentsEventStore()
        {
            var storageEngine = await CreateStorageEngine(PaymentsBucket);
            return new EventStore(storageEngine);
        }

        protected abstract Task<IStorageEngine> CreateStorageEngine(string bucket);
    }
}