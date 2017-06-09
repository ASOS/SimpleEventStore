using System;
using System.Linq;
using System.Threading.Tasks;
using SimpleEventStore.Tests.Events;
using SimpleEventStore.Tests.Metadata;
using Xunit;

namespace SimpleEventStore.Tests
{
    public abstract class EventStoreAppending : EventStoreTestBase
    {
        [Fact]
        public async Task when_appending_to_a_new_stream_the_event_is_saved()
        {
            var streamId = Guid.NewGuid().ToString();
            var subject = await GetEventStore();
            var @event = new EventData(Guid.NewGuid(), new OrderCreated(streamId));
            
            await subject.AppendToStream(streamId, 0, @event);

            var stream = await subject.ReadStreamForwards(streamId);
            Assert.Equal(1, stream.Count());
            Assert.Equal(streamId, stream.Single().StreamId);
            Assert.Equal(@event.EventId, stream.Single().EventId);
            Assert.Equal(1, stream.Single().EventNumber);
        }

        [Fact]
        public async Task when_appending_to_an_existing_stream_the_event_is_saved()
        {
            var streamId = Guid.NewGuid().ToString();
            var subject = await GetEventStore();
            await subject.AppendToStream(streamId, 0, new EventData(Guid.NewGuid(), new OrderCreated(streamId)));
            var @event = new EventData(Guid.NewGuid(), new OrderDispatched(streamId));

            await subject.AppendToStream(streamId, 1, @event);

            var stream = await subject.ReadStreamForwards(streamId);
            Assert.Equal(2, stream.Count());
            Assert.Equal(@event.EventId, stream.Skip(1).Single().EventId);
            Assert.Equal(2, stream.Skip(1).Single().EventNumber);
        }

        [Theory]
        [InlineData(-1)]
        [InlineData(1)]
        public async Task when_appending_to_a_new_stream_with_an_unexpected_version__a_concurrency_error_is_thrown(int expectedVersion)
        {
            var streamId = Guid.NewGuid().ToString();
            var subject = await GetEventStore();
            var @event = new EventData(Guid.NewGuid(), new OrderDispatched(streamId));

            await Assert.ThrowsAsync<ConcurrencyException>(async () => await subject.AppendToStream(streamId, expectedVersion, @event));
        }

        [Theory]
        [InlineData(0)]
        [InlineData(2)]
        public async Task when_appending_to_an_existing_stream_with_an_unexpected_version_a_concurrency_error_is_thrown(int expectedVersion)
        {
            var streamId = Guid.NewGuid().ToString();
            var subject = await GetEventStore();
            await subject.AppendToStream(streamId, 0, new EventData(Guid.NewGuid(), new OrderCreated(streamId)));

            var @event = new EventData(Guid.NewGuid(), new OrderDispatched(streamId));

            await Assert.ThrowsAsync<ConcurrencyException>(async () => await subject.AppendToStream(streamId, expectedVersion, @event));
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData(" ")]
        public async Task when_appending_to_an_invalid_stream_id_an_argument_error_is_thrown(string streamId)
        {
            var eventStore = await GetEventStore();
            await Assert.ThrowsAsync<ArgumentException>(async () => await eventStore.AppendToStream(streamId, 0, new EventData(Guid.NewGuid(), new OrderCreated(streamId))));
        }

        [Fact]
        public async Task when_appending_to_a_new_stream_with_multiple_events_then_they_are_saved()
        {
            var streamId = Guid.NewGuid().ToString();
            var subject = await GetEventStore();
            var @events = new []
            {
                new EventData(Guid.NewGuid(), new OrderCreated(streamId)),
                new EventData(Guid.NewGuid(), new OrderDispatched(streamId))
            };

            await subject.AppendToStream(streamId, 0, @events);

            var savedEvents = await subject.ReadStreamForwards(streamId);

            Assert.Equal(2, savedEvents.Count());
            Assert.Equal(streamId, savedEvents.First().StreamId);
            Assert.Equal(1, savedEvents.First().EventNumber);
            Assert.Equal(streamId, savedEvents.Skip(1).Single().StreamId);
            Assert.Equal(2, savedEvents.Skip(1).Single().EventNumber);
        }

        [Fact]
        public async Task when_appending_to_a_new_stream_the_event_metadata_is_saved()
        {
            var streamId = Guid.NewGuid().ToString();
            var subject = await GetEventStore();
            var metadata = new TestMetadata { Value = "Hello" };
            var @event = new EventData(Guid.NewGuid(), new OrderCreated(streamId), metadata);

            await subject.AppendToStream(streamId, 0, @event);

            var stream = await subject.ReadStreamForwards(streamId);
            Assert.Equal(metadata.Value, ((TestMetadata)stream.Single().Metadata).Value);
        }
    }
}
