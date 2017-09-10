using Microsoft.Azure.Documents;

namespace SimpleEventStore.AzureDocumentDb
{
    public class CollectionOptions
    {
        public CollectionOptions()
        {
            this.ConsistencyLevel = ConsistencyLevel.Session;
            this.CollectionRequestUnits = 400;
            this.CollectionName = "Commits";
            this.Bucket = AzureDocumentDbStorageEngine.DefaultBucket;
        }

        public string CollectionName { get; set; }

        public ConsistencyLevel ConsistencyLevel { get; set; }

        public int CollectionRequestUnits { get; set; }

        public string Bucket { get; set; }
    }
}