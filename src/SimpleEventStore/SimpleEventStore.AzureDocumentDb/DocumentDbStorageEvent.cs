using System;
using Microsoft.Azure.Documents;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace SimpleEventStore.AzureDocumentDb
{
    public class DocumentDbStorageEvent
    {
        [JsonProperty("id")]
        public string Id { get; set;  }

        [JsonProperty("eventId")]
        public Guid EventId { get; set; }

        [JsonProperty("body")]
        public JObject Body { get; set; }

        [JsonProperty("bodyType")]
        public string BodyType { get; set; }

        [JsonProperty("metadata")]
        public JObject Metadata { get; set; }

        [JsonProperty("metadataType")]
        public string MetadataType { get; set; }

        [JsonProperty("streamId")]
        public string StreamId { get; set; }

        [JsonProperty("eventNumber")]
        public int EventNumber { get; set; }

        [JsonProperty("bucket")]
        public string Bucket { get; set; }

        public static DocumentDbStorageEvent FromStorageEvent(StorageEvent @event, ISerializationTypeMap typeMap, string bucket)
        {
            var docDbEvent = new DocumentDbStorageEvent
            {
                Id = $"{@event.StreamId}:{bucket}:{@event.EventNumber}",
                EventId = @event.EventId,
                Body = JObject.FromObject(@event.EventBody),
                BodyType = typeMap.GetNameFromType(@event.EventBody.GetType()),
                Bucket = bucket
            };

            if (@event.Metadata != null)
            {
                docDbEvent.Metadata = JObject.FromObject(@event.Metadata);
                docDbEvent.MetadataType = typeMap.GetNameFromType(@event.Metadata.GetType());
            }
            docDbEvent.StreamId = @event.StreamId;
            docDbEvent.EventNumber = @event.EventNumber;

            return docDbEvent;
        }

        public static DocumentDbStorageEvent FromDocument(Document document)
        {
            var docDbEvent = new DocumentDbStorageEvent
            {
                Id = document.GetPropertyValue<string>("id"),
                EventId = document.GetPropertyValue<Guid>("eventId"),
                Body = document.GetPropertyValue<JObject>("body"),
                BodyType = document.GetPropertyValue<string>("bodyType"),
                Metadata = document.GetPropertyValue<JObject>("metadata"),
                MetadataType = document.GetPropertyValue<string>("metadataType"),
                StreamId = document.GetPropertyValue<string>("streamId"),
                EventNumber = document.GetPropertyValue<int>("eventNumber"),
                Bucket = document.GetPropertyValue<string>("bucket")
            };

            return docDbEvent;
        }

        public StorageEvent ToStorageEvent(ISerializationTypeMap typeMap)
        {
            object body = Body.ToObject(typeMap.GetTypeFromName(BodyType));
            object metadata = Metadata?.ToObject(typeMap.GetTypeFromName(MetadataType));
            return new StorageEvent(StreamId, new EventData(EventId, body, metadata), EventNumber);
        }
    }
}