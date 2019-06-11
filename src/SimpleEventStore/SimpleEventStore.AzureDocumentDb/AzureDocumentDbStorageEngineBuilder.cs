using System;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;

namespace SimpleEventStore.AzureDocumentDb
{
    public class AzureDocumentDbStorageEngineBuilder
    {
        private readonly string databaseName;
        private readonly DocumentClient client;
        private readonly CollectionOptions collectionOptions = new CollectionOptions();
        private readonly DatabaseOptions databaseOptions = new DatabaseOptions();
        private readonly LoggingOptions loggingOptions = new LoggingOptions();
        private ISerializationTypeMap typeMap = new DefaultSerializationTypeMap();
        private JsonSerializerSettings jsonSerializerSettings = new JsonSerializerSettings();

        public AzureDocumentDbStorageEngineBuilder(DocumentClient client, string databaseName)
        {
            Guard.IsNotNull(nameof(client), client);
            Guard.IsNotNullOrEmpty(nameof(databaseName), databaseName);

            this.client = client;
            this.databaseName = databaseName;
        }

        public AzureDocumentDbStorageEngineBuilder UseCollection(Action<CollectionOptions> action)
        {
            Guard.IsNotNull(nameof(action), action);

            action(collectionOptions);
            return this;
        }

        public AzureDocumentDbStorageEngineBuilder UseLogging(Action<LoggingOptions> action)
        {
            Guard.IsNotNull(nameof(action), action);

            action(loggingOptions);
            return this;
        }

        public AzureDocumentDbStorageEngineBuilder UseTypeMap(ISerializationTypeMap typeMap)
        {
            Guard.IsNotNull(nameof(typeMap), typeMap);
            this.typeMap = typeMap;

            return this;
        }

        public AzureDocumentDbStorageEngineBuilder UseJsonSerializerSettings(JsonSerializerSettings settings)
        {
            Guard.IsNotNull(nameof(settings), settings);
            this.jsonSerializerSettings = settings;
            return this;
        }

        public AzureDocumentDbStorageEngineBuilder UseSharedThroughput(Action<DatabaseOptions> action)
        {
            Guard.IsNotNull(nameof(action), action);

            action(databaseOptions);
            return this;
        }

        public IStorageEngine Build()
        {
            if (this.collectionOptions.CollectionRequestUnits == null && this.databaseOptions.DatabaseRequestUnits == null)
            {
                throw new ArgumentException("Request units must be set in at least one location");
            }

            if (this.collectionOptions.CollectionRequestUnits > this.databaseOptions.DatabaseRequestUnits)
            {
                throw new ArgumentException("Unable to allocate more RUs to the collection than database");
            }

            var engine = new AzureDocumentDbStorageEngine(this.client, this.databaseName, this.collectionOptions,this.databaseOptions, this.loggingOptions, this.typeMap, JsonSerializer.Create(this.jsonSerializerSettings));
            return engine;
        }
    }
}
