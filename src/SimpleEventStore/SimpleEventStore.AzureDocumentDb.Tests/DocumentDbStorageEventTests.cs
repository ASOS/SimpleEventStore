using System;
using System.Reflection;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using SimpleEventStore.Tests.Events;
using NUnit.Framework;

namespace SimpleEventStore.AzureDocumentDb.Tests
{
    [TestFixture]
    public class DocumentDbStorageEventTests
    {
        [Test]
        public void when_converting_to_a_storage_event_it_succeeds()
        {
            var id = Guid.NewGuid();
            var body = new OrderCreated("TEST-ORDER");
            var metadata = new TestMetadata { Value = "TEST-VALUE" };
            var sut = new DocumentDbStorageEvent
            {
                StreamId = "TEST-STREAM",
                Body = JObject.FromObject(body),
                BodyType = "OrderCreated",
                Metadata = JObject.FromObject(metadata),
                MetadataType = "TestMetadata",
                EventNumber = 1,
                EventId = id
            };
            var typeMap = new ConfigurableSerializationTypeMap().RegisterTypes(
                typeof(OrderCreated).GetTypeInfo().Assembly,
                t => t.Namespace != null && t.Namespace.EndsWith("Events"),
                t => t.Name);
            var result = sut.ToStorageEvent(typeMap, JsonSerializer.CreateDefault());

            Assert.That(result.StreamId, Is.EqualTo(sut.StreamId));
            Assert.That(((OrderCreated)result.EventBody).OrderId, Is.EqualTo(body.OrderId));
            Assert.That(((TestMetadata)result.Metadata).Value, Is.EqualTo(metadata.Value));
            Assert.That(result.EventNumber, Is.EqualTo(sut.EventNumber));
            Assert.That(result.EventId, Is.EqualTo(sut.EventId));
        }
    }
}
