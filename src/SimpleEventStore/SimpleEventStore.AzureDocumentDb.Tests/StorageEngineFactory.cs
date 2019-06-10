using System;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using SimpleEventStore.Tests.Events;

namespace SimpleEventStore.AzureDocumentDb.Tests
{
    internal static class StorageEngineFactory
    {
        internal static Task<IStorageEngine> Create(string databaseName, Action<CollectionOptions> collectionOverrides = null)
        {
            return Create(databaseName, new JsonSerializerSettings(), collectionOverrides);
        }

        internal static Task<IStorageEngine> Create(string databaseName, JsonSerializerSettings settings, Action<CollectionOptions> collectionOverrides = null)
        {
            var config = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json")
                .Build();

            var consistencyLevel = config["ConsistencyLevel"];

            if(!Enum.TryParse(consistencyLevel, true, out ConsistencyLevel consistencyLevelEnum))
            {
                throw new Exception($"The ConsistencyLevel value {consistencyLevel} is not supported");
            }

            var client = DocumentClientFactory.Create(databaseName, settings);

            return new AzureDocumentDbStorageEngineBuilder(client, databaseName)
                .UseCollection(o =>
                {
                    o.ConsistencyLevel = consistencyLevelEnum;
                    o.CollectionRequestUnits = TestConstants.RequestUnits;
                    collectionOverrides?.Invoke(o);
                })
                .UseTypeMap(new ConfigurableSerializationTypeMap()
                    .RegisterTypes(
                        typeof(OrderCreated).GetTypeInfo().Assembly,
                        t => t.Namespace != null && t.Namespace.EndsWith("Events"),
                        t => t.Name))
                .UseJsonSerializerSettings(settings)
                .Build()
                .Initialise();
        }
    }
}
