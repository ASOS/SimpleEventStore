using System;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Extensions.Configuration;
using SimpleEventStore.Tests.Events;

namespace SimpleEventStore.AzureDocumentDb.Tests
{
    internal static class StorageEngineFactory
    {
        internal static async Task<IStorageEngine> Create(string databaseName, string bucket = AzureDocumentDbStorageEngine.DefaultBucket)
        {
            var config = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json")
                .Build();

            var documentDbUri = config["Uri"];
            var authKey = config["AuthKey"];
            var consistencyLevel = config["ConsistencyLevel"];
            ConsistencyLevel consistencyLevelEnum;

            if(!Enum.TryParse(consistencyLevel, true, out consistencyLevelEnum))
            {
                throw new Exception($"The ConsistencyLevel value {consistencyLevel} is not supported");
            }

            DocumentClient client = new DocumentClient(new Uri(documentDbUri), authKey);

            return await new AzureDocumentDbStorageEngineBuilder(client, databaseName)
                .UseCollection(o =>
                {
                    o.ConsistencyLevel = consistencyLevelEnum;
                    o.CollectionRequestUnits = 400;
                    o.Bucket = bucket;
                })
                .UseTypeMap(new ConfigurableSerializationTypeMap()
                    .RegisterTypes(
                        typeof(OrderCreated).GetTypeInfo().Assembly,
                        t => t.Namespace != null && t.Namespace.EndsWith("Events"),
                        t => t.Name))
                .Build()
                .Initialise();
        }
    }
}
