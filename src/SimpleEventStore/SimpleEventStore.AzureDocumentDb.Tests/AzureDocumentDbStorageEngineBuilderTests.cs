using System;
using Microsoft.Azure.Documents.Client;
using NUnit.Framework;

namespace SimpleEventStore.AzureDocumentDb.Tests
{
    [TestFixture]
    public class AzureDocumentDbStorageEngineBuilderTests
    {
        [Test]
        public void when_creating_an_instance_the_document_client_must_be_supplied()
        {
            Assert.Throws<ArgumentNullException>(() => new AzureDocumentDbStorageEngineBuilder(null, "Test"));
        }

        [Test]
        public void when_creating_an_instance_the_database_name_must_be_supplied()
        {
            Assert.Throws<ArgumentException>(() => new AzureDocumentDbStorageEngineBuilder(CreateClient(), null));
        }

        [Test]
        public void when_setting_collection_settings_a_callback_must_be_supplied()
        {
            var builder = new AzureDocumentDbStorageEngineBuilder(CreateClient(), "Test");
            Assert.Throws<ArgumentNullException>(() => builder.UseCollection(null));
        }

        [Test]
        public void when_setting_subscription_settings_a_callback_must_be_supplied()
        {
            var builder = new AzureDocumentDbStorageEngineBuilder(CreateClient(), "Test");
            Assert.Throws<ArgumentNullException>(() => builder.UseCollection(null));
        }

        [Test]
        public void when_setting_logging_settings_a_callback_must_be_supplied()
        {
            var builder = new AzureDocumentDbStorageEngineBuilder(CreateClient(), "Test");
            Assert.Throws<ArgumentNullException>(() => builder.UseLogging(null));
        }

        [Test]
        public void when_setting_the_type_map_it_must_be_supplied()
        {
            var builder = new AzureDocumentDbStorageEngineBuilder(CreateClient(), "Test");
            Assert.Throws<ArgumentNullException>(() => builder.UseTypeMap(null));
        }

        [Test]
        public void when_setting_the_jsonserializationsettings_it_must_be_supplied()
        {
            var builder = new AzureDocumentDbStorageEngineBuilder(CreateClient(), "Test");
            Assert.Throws<ArgumentNullException>(() => builder.UseJsonSerializerSettings(null));
        }

        [Test]
        public void throughput_must_be_set_in_one_location()
        {
            var builder = new AzureDocumentDbStorageEngineBuilder(CreateClient(), "Test")
                .UseSharedThroughput(o => { o.DatabaseRequestUnits = null; })
                .UseCollection(o => o.CollectionRequestUnits = null);

            Assert.Throws<ArgumentException>(() => builder.Build());
        }

        [Test]
        public void collection_throughput_cannot_be_greater_than_database_throughput()
        {
            var builder = new AzureDocumentDbStorageEngineBuilder(CreateClient(), "Test")
                .UseSharedThroughput(o => { o.DatabaseRequestUnits = 400; })
                .UseCollection(o => o.CollectionRequestUnits = 500);

            Assert.Throws<ArgumentException>(() => builder.Build());
        }

        private static DocumentClient CreateClient()
        {
            var client = new DocumentClient(new Uri("https://localhost:8081/"), "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==");
            return client;
        }
    }
}
