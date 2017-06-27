using System;
using System.Linq;
using System.Threading.Tasks;
using SimpleEventStore.Tests.Events;
using Xunit;
using System.Collections.Generic;

namespace SimpleEventStore.Tests
{
    // TODOs
    // 1. Make partioning support configurable
    // 2. Allow for lower levels of consistency than just strong

    public abstract class EventStoreReading : EventStoreTestBase
    {
        [Fact]
        public async Task when_reading_a_stream_all_events_are_returned()
        {
            var streamId = Guid.NewGuid().ToString();
            var subject = await GetEventStore();

            await subject.AppendToStream(streamId, 0, new EventData(Guid.NewGuid(), new OrderCreated(streamId)));
            await subject.AppendToStream(streamId, 1, new EventData(Guid.NewGuid(), new OrderDispatched(streamId)));

            var events = await subject.ReadStreamForwards(streamId);

            Assert.Equal(2, events.Count());
            Assert.IsType<OrderCreated>(events.First().EventBody);
            Assert.IsType<OrderDispatched>(events.Skip(1).Single().EventBody);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData(" ")]
        public async Task when_reading_from_an_invalid_stream_id_an_argument_error_is_thrown(string streamId)
        {
            var eventStore = await GetEventStore();
            await Assert.ThrowsAsync<ArgumentException>(async () => await eventStore.ReadStreamForwards(streamId));
        }

        [Fact]
        public async Task when_reading_a_stream_only_the_required_events_are_returned()
        {
            var streamId = Guid.NewGuid().ToString();
            var subject = await GetEventStore();

            await subject.AppendToStream(streamId, 0, new EventData(Guid.NewGuid(), new OrderCreated(streamId)));
            await subject.AppendToStream(streamId, 1, new EventData(Guid.NewGuid(), new OrderDispatched(streamId)));

            var events = await subject.ReadStreamForwards(streamId, startPosition: 2, numberOfEventsToRead: 1);

            Assert.Equal(1, events.Count());
            Assert.IsType<OrderDispatched>(events.First().EventBody);
        }

        [Fact]
        public async Task when_reading_all_streams_with_no_checkpoint_all_events_are_returned()
        {
            var stream1Id = Guid.NewGuid().ToString();
            var stream2Id = Guid.NewGuid().ToString();
            var subject = await GetEventStore();

            await subject.AppendToStream(stream1Id, 0, new EventData(Guid.NewGuid(), new OrderCreated(stream1Id)));
            await subject.AppendToStream(stream2Id, 0, new EventData(Guid.NewGuid(), new OrderDispatched(stream2Id)));

            List<StorageEvent> results = new List<StorageEvent>();
            await subject.ReadAllForwards(
                (events, checkpoint) =>
                {
                    results.AddRange(
                        events.Where(
                            @event => @event.StreamId == stream1Id || @event.StreamId == stream2Id));
                    return Task.CompletedTask;
                },
                null);

            Assert.Equal(2, results.Count());
            Assert.IsType<OrderCreated>(results.First().EventBody);
            Assert.IsType<OrderDispatched>(results.Last().EventBody);
        }

        [Fact]
        public async Task when_reading_all_streams_with_a_checkpoint_only_events_after_checkpoint_are_returned()
        {
            var stream1Id = Guid.NewGuid().ToString();
            var stream2Id = Guid.NewGuid().ToString();
            var subject = await GetEventStore();

            await subject.AppendToStream(stream1Id, 0, new EventData(Guid.NewGuid(), new OrderCreated(stream1Id)));
            await subject.AppendToStream(stream2Id, 0, new EventData(Guid.NewGuid(), new OrderDispatched(stream2Id)));

            string initialCheckpoint = null;
            await subject.ReadAllForwards(
                (events, checkpoint) =>
                {
                    initialCheckpoint = checkpoint;
                    return Task.CompletedTask;
                },
                null);

            await subject.AppendToStream(stream1Id, 1, new EventData(Guid.NewGuid(), new OrderCreated(stream1Id)));
            await subject.AppendToStream(stream2Id, 1, new EventData(Guid.NewGuid(), new OrderDispatched(stream2Id)));

            List<StorageEvent> results = new List<StorageEvent>();
            await subject.ReadAllForwards(
                (events, checkpoint) =>
                {
                    results.AddRange(
                        events.Where(
                            @event => @event.StreamId == stream1Id || @event.StreamId == stream2Id));
                    return Task.CompletedTask;
                },
                initialCheckpoint);

            Assert.Equal(2, results.Count());
            Assert.IsType<OrderCreated>(results.First().EventBody);
            Assert.IsType<OrderDispatched>(results.Last().EventBody);
        }
    }
}
