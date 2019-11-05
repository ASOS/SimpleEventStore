using NUnit.Framework;
using SimpleEventStore.Tests.Events;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleEventStore.Tests
{
    // TODOs
    // 1. Make partioning support configurable
    // 2. Allow for lower levels of consistency than just strong
    [TestFixture]
    public abstract class EventStoreReading : EventStoreTestBase
    {
        [Test]
        public async Task when_reading_a_stream_which_has_no_events_an_empty_list_is_returned()
        {
            var streamId = Guid.NewGuid().ToString();
            var subject = await GetEventStore();

            var events = await subject.ReadStreamForwards(streamId);

            Assert.That(events.Count, Is.EqualTo(0));
        }

        [Test]
        public async Task when_reading_a_stream_all_events_are_returned()
        {
            var streamId = Guid.NewGuid().ToString();
            var subject = await GetEventStore();

            await subject.AppendToStream(streamId, 0, new EventData(Guid.NewGuid(), new OrderCreated(streamId)));
            await subject.AppendToStream(streamId, 1, new EventData(Guid.NewGuid(), new OrderDispatched(streamId)));

            var events = await subject.ReadStreamForwards(streamId);

            Assert.That(events.Count, Is.EqualTo(2));
            Assert.That(events.First().EventBody, Is.InstanceOf<OrderCreated>());
            Assert.That(events.Skip(1).Single().EventBody, Is.InstanceOf<OrderDispatched>());
        }

        [Test]
        [TestCase(null)]
        [TestCase("")]
        [TestCase(" ")]
        public async Task when_reading_from_an_invalid_stream_id_an_argument_error_is_thrown(string streamId)
        {
            var eventStore = await GetEventStore();
            Assert.ThrowsAsync<ArgumentException>(async () => await eventStore.ReadStreamForwards(streamId));
        }

        [Test]
        public async Task when_reading_a_stream_only_the_required_events_are_returned()
        {
            var streamId = Guid.NewGuid().ToString();
            var subject = await GetEventStore();

            await subject.AppendToStream(streamId, 0, new EventData(Guid.NewGuid(), new OrderCreated(streamId)));
            await subject.AppendToStream(streamId, 1, new EventData(Guid.NewGuid(), new OrderDispatched(streamId)));

            var events = await subject.ReadStreamForwards(streamId, startPosition: 2, numberOfEventsToRead: 1);

            Assert.That(events.Count, Is.EqualTo(1));
            Assert.That(events.First().EventBody, Is.InstanceOf<OrderDispatched>());
        }

        [Test]
        public async Task when_reading_a_stream_the_engine_honours_cancellation_token()
        {
            var streamId = Guid.NewGuid().ToString();
            var subject = await GetEventStore();

            using (var cts = new CancellationTokenSource())
            {
                cts.Cancel();

                AsyncTestDelegate act = () => subject.ReadStreamForwards(streamId, cts.Token);

                Assert.That(act, Throws.InstanceOf<OperationCanceledException>());
            }
        }
    }
}
