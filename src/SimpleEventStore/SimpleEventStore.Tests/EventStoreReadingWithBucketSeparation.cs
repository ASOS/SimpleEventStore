using System;
using System.Linq;
using System.Threading.Tasks;
using SimpleEventStore.Tests.Events;
using Xunit;

namespace SimpleEventStore.Tests
{
    public abstract class EventStoreReadingWithBucketSeparation : EventStoreWithBucketBase
    {
        protected abstract Task AppendDocument(StorageEvent document);

        [Fact]
        public async Task existing_events_stored_without_a_bucket_are_included_when_a_bucket_is_not_specified_for_retreival()
        {
            var streamId = Guid.NewGuid().ToString();
            await AppendDocument(new StorageEvent(streamId, new EventData(Guid.NewGuid(), new OrderCreated(streamId)), 1));

            var subject = await GetEventStoreWithNoBucketSpecified();

            await subject.AppendToStream(streamId, 1, new EventData(Guid.NewGuid(), new OrderDispatched(streamId)));

            var events = await subject.ReadStreamForwards(streamId);

            Assert.Equal(2, events.Count);
            Assert.IsType<OrderCreated>(events.First().EventBody);
            Assert.IsType<OrderDispatched>(events.Skip(1).Single().EventBody);
        }

        [Fact]
        public async Task events_stored_without_a_bucket_but_with_different_streams_are_kept_isolated()
        {
            var firstStream = Guid.NewGuid().ToString();
            await AppendDocument(new StorageEvent(firstStream, new EventData(Guid.NewGuid(), new OrderCreated(firstStream)), 1));

            var secondStream = Guid.NewGuid().ToString();
            await AppendDocument(new StorageEvent(secondStream, new EventData(Guid.NewGuid(), new OrderCreated(secondStream)), 1));

            var subject = await GetEventStoreWithNoBucketSpecified();

            Assert.Equal(1, (await subject.ReadStreamForwards(firstStream)).Count);
            Assert.Equal(1, (await subject.ReadStreamForwards(secondStream)).Count);
        }

        [Fact]
        public async Task when_reading_a_stream_only_the_required_events_are_returned()
        {
            var streamId = Guid.NewGuid().ToString();
            var subject = await GetEventStoreWithNoBucketSpecified();

            await AppendDocument(new StorageEvent(streamId, new EventData(Guid.NewGuid(), new OrderCreated(streamId)), 1));
            await AppendDocument(new StorageEvent(streamId, new EventData(Guid.NewGuid(), new OrderDispatched(streamId)), 2));

            var events = await subject.ReadStreamForwards(streamId, startPosition: 2, numberOfEventsToRead: 1);

            Assert.Equal(1, events.Count);
            Assert.IsType<OrderDispatched>(events.First().EventBody);
        }
    }
}
