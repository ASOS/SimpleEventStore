using System;
using Microsoft.Azure.Documents.Client;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;

namespace SimpleEventStore.AzureDocumentDb.Tests
{
    internal static class DocumentClientFactory
    {
        internal static DocumentClient Create(string databaseName)
        {
            return Create(databaseName, new JsonSerializerSettings());
        }

        internal static DocumentClient Create(string databaseName, JsonSerializerSettings settings)
        {
            var config = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json")
                .Build();

            var documentDbUri = config["Uri"];
            var authKey = config["AuthKey"];

            return new DocumentClient(new Uri(documentDbUri), authKey, settings);
        }
    }
}