using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using SimpleEventStore.Tests.Events;

namespace SimpleEventStore.Tests
{
    [TestFixture]
    public abstract class EventStoreAppending : EventStoreTestBase
    {
        [Test]
        public async Task when_appending_to_a_new_stream_the_event_is_saved()
        {
            var streamId = Guid.NewGuid().ToString();
            var subject = await GetEventStore();
            var @event = new EventData(Guid.NewGuid(), new OrderCreated(streamId));
            
            await subject.AppendToStream(streamId, 0, @event);

            var stream = await subject.ReadStreamForwards(streamId);
            Assert.That(stream.Count, Is.EqualTo(1));
            Assert.That(stream.Single().StreamId, Is.EqualTo(streamId));
            Assert.That(stream.Single().EventId, Is.EqualTo(@event.EventId));
            Assert.That(stream.Single().EventNumber, Is.EqualTo(1));
        }

        [Test]
        public async Task when_appending_to_an_existing_stream_the_event_is_saved()
        {
            var streamId = Guid.NewGuid().ToString();
            var subject = await GetEventStore();
            await subject.AppendToStream(streamId, 0, new EventData(Guid.NewGuid(), new OrderCreated(streamId)));
            var @event = new EventData(Guid.NewGuid(), new OrderDispatched(streamId));

            await subject.AppendToStream(streamId, 1, @event);

            var stream = await subject.ReadStreamForwards(streamId);
            Assert.That(stream.Count, Is.EqualTo(2));
            Assert.That(stream.Skip(1).Single().EventId, Is.EqualTo(@event.EventId));
            Assert.That(stream.Skip(1).Single().EventNumber, Is.EqualTo(2));
        }

        [Test]
        [TestCase(-1)]
        [TestCase(1)]
        public async Task when_appending_to_a_new_stream_with_an_unexpected_version__a_concurrency_error_is_thrown(int expectedVersion)
        {
            var streamId = Guid.NewGuid().ToString();
            var subject = await GetEventStore();
            var @event = new EventData(Guid.NewGuid(), new OrderDispatched(streamId));

            Assert.ThrowsAsync<ConcurrencyException>(async () => await subject.AppendToStream(streamId, expectedVersion, @event));
        }

        [Test]
        [TestCase(0)]
        [TestCase(2)]
        public async Task when_appending_to_an_existing_stream_with_an_unexpected_version_a_concurrency_error_is_thrown(int expectedVersion)
        {
            var streamId = Guid.NewGuid().ToString();
            var subject = await GetEventStore();
            await subject.AppendToStream(streamId, 0, new EventData(Guid.NewGuid(), new OrderCreated(streamId)));

            var @event = new EventData(Guid.NewGuid(), new OrderDispatched(streamId));

            Assert.ThrowsAsync<ConcurrencyException>(async () => await subject.AppendToStream(streamId, expectedVersion, @event));
        }

        [Test]
        [TestCase(null)]
        [TestCase("")]
        [TestCase(" ")]
        public async Task when_appending_to_an_invalid_stream_id_an_argument_error_is_thrown(string streamId)
        {
            var eventStore = await GetEventStore();
            Assert.ThrowsAsync<ArgumentException>(async () => await eventStore.AppendToStream(streamId, 0, new EventData(Guid.NewGuid(), new OrderCreated(streamId))));
        }

        [Test]
        public async Task when_appending_to_a_new_stream_with_multiple_events_then_they_are_saved()
        {
            var streamId = Guid.NewGuid().ToString();
            var subject = await GetEventStore();
            var events = new []
            {
                new EventData(Guid.NewGuid(), new OrderCreated(streamId)),
                new EventData(Guid.NewGuid(), new OrderDispatched(streamId))
            };

            await subject.AppendToStream(streamId, 0, events);

            var savedEvents = await subject.ReadStreamForwards(streamId);

            Assert.That(savedEvents.Count, Is.EqualTo(2));
            Assert.That(savedEvents.First().StreamId, Is.EqualTo(streamId));
            Assert.That(savedEvents.First().EventNumber, Is.EqualTo(1));
            Assert.That(savedEvents.Skip(1).Single().StreamId, Is.EqualTo(streamId));
            Assert.That(savedEvents.Skip(1).Single().EventNumber, Is.EqualTo(2));
        }

        [Test]
        public async Task when_appending_to_a_new_stream_the_event_metadata_is_saved()
        {
            var streamId = Guid.NewGuid().ToString();
            var subject = await GetEventStore();
            var metadata = new TestMetadata { Value = "Hello" };
            var @event = new EventData(Guid.NewGuid(), new OrderCreated(streamId), metadata);

            await subject.AppendToStream(streamId, 0, @event);

            var stream = await subject.ReadStreamForwards(streamId);
            Assert.That(((TestMetadata)stream.Single().Metadata).Value, Is.EqualTo(metadata.Value));
        }

        [Test]
        public async Task when_appending_to_a_stream_the_engine_honours_cancellation_token()
        {
            var streamId = Guid.NewGuid().ToString();
            var subject = await GetEventStore();
            var metadata = new TestMetadata { Value = "Hello" };
            var @event = new EventData(Guid.NewGuid(), new OrderCreated(streamId), metadata);

            using (var cts = new CancellationTokenSource())
            {
                cts.Cancel();

                AsyncTestDelegate act = () => subject.AppendToStream(streamId, 0, cts.Token, @event);

                Assert.That(act, Throws.InstanceOf<OperationCanceledException>());
            }
        }
    }
}
