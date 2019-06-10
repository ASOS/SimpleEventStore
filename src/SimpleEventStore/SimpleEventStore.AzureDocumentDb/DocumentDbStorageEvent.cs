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

        public static DocumentDbStorageEvent FromStorageEvent(StorageEvent @event, ISerializationTypeMap typeMap, JsonSerializer serializer)
        {
            var docDbEvent = new DocumentDbStorageEvent();
            docDbEvent.Id = $"{@event.StreamId}:{@event.EventNumber}";
            docDbEvent.EventId = @event.EventId;
            docDbEvent.Body = JObject.FromObject(@event.EventBody, serializer);
            docDbEvent.BodyType = typeMap.GetNameFromType(@event.EventBody.GetType());
            if (@event.Metadata != null)
            {
                docDbEvent.Metadata = JObject.FromObject(@event.Metadata, serializer);
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
                EventNumber = document.GetPropertyValue<int>("eventNumber")
            };

            return docDbEvent;
        }

        public StorageEvent ToStorageEvent(ISerializationTypeMap typeMap, JsonSerializer serializer)
        {
            object body = Body.ToObject(typeMap.GetTypeFromName(BodyType), serializer);
            object metadata = Metadata?.ToObject(typeMap.GetTypeFromName(MetadataType), serializer);
            return new StorageEvent(StreamId, new EventData(EventId, body, metadata), EventNumber);
        }
    }
}