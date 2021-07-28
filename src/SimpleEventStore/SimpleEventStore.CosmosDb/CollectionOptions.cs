using Microsoft.Azure.Cosmos;

namespace SimpleEventStore.CosmosDb
{
    public class CollectionOptions
    {
        public CollectionOptions()
        {
            ConsistencyLevel = ConsistencyLevel.Session;
            CollectionRequestUnits = 400;
            CollectionName = "Commits";
        }

        public string CollectionName { get; set; }

        public ConsistencyLevel ConsistencyLevel { get; set; }

        public int? CollectionRequestUnits { get; set; }

        public bool UseAutoscale { get; set; }

        public int? DefaultTimeToLive { get; set; }
    }
}