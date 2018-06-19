using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using NUnit.Framework;
using SimpleEventStore.Tests;
using SimpleEventStore.Tests.Events;

namespace SimpleEventStore.AzureDocumentDb.Tests
{
    [TestFixture]
    public class AzureDocumentDbEventStoreAppendingWithConverters : EventStoreTestBase
    {
        [Test]
        public async Task when_appending_an_event_that_requires_a_converter_the_event_is_saved_and_read()
        {
            var streamId = Guid.NewGuid().ToString();
            var subject = await GetEventStore();
            var @event = new EventData(Guid.NewGuid(), new OrderProcessed(streamId, new Version(1, 2, 0)));
            
            await subject.AppendToStream(streamId, 0, @event);

            var stream = await subject.ReadStreamForwards(streamId);
            Assert.That(stream.Count, Is.EqualTo(1));
            Assert.That(stream.Single().StreamId, Is.EqualTo(streamId));
            Assert.That(stream.Single().EventId, Is.EqualTo(@event.EventId));
            Assert.That(stream.Single().EventNumber, Is.EqualTo(1));
        }

        protected override Task<IStorageEngine> CreateStorageEngine()
        {
            return StorageEngineFactory.Create("JsonSerializationSettingsTests",
                new JsonSerializerSettings
                {
                    Converters = new List<JsonConverter>
                    {
                        new VersionConverter()
                    }
                });
        }
    }
}
