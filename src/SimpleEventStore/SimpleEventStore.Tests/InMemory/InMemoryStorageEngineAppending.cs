using NUnit.Framework;
using SimpleEventStore.InMemory;
using SimpleEventStore.Tests.Events;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace SimpleEventStore.Tests.InMemory
{
    [TestFixture]
    public class InMemoryStorageEngineAppending
    {
        private string _streamId;
        private InMemoryStorageEngine _engine;
        private StorageEvent _secondEvent;

        [SetUp]
        public void Setup()
        {
            _streamId = Guid.NewGuid().ToString();
            _engine = new InMemoryStorageEngine();
            _secondEvent = new StorageEvent(
                _streamId,
                new EventData(
                    Guid.NewGuid(),
                    new OrderCreated(_streamId)),
                2);
        }

        [Test]
        public void when_appending_to_stream_and_conflict_check_is_disabled_then_tolerate_events_missing_from_storage_to_permit_setup_of_ttl_unit_test_scenarios()
        {
            Assert.DoesNotThrowAsync(() =>
                _engine.AppendToStream(
                    _streamId,
                    new[] { _secondEvent },
                    ConcurrencyCheck.AllowMissingAndDuplicatedEventNumbersAndAppendRegardless));
        }

        [Test]
        public async Task when_appending_to_stream_and_conflict_check_is_disabled_conflicting_events_are_stored_and_can_be_retrieved()
        {
            await _engine.AppendToStream(
                _streamId,
                new[] { _secondEvent },
                ConcurrencyCheck.AllowMissingAndDuplicatedEventNumbersAndAppendRegardless);

            var actualStreamContent = await _engine.ReadStreamForwards(_streamId, 0, 2);
            Assert.AreEqual(1, actualStreamContent.Count());
        }
    }
}
