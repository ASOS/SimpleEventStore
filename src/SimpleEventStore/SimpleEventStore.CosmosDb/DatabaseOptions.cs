namespace SimpleEventStore.CosmosDb
{
    public class DatabaseOptions
    {
        public int? DatabaseRequestUnits { get; set; }

        public bool UseAutoscale { get; set; }
    }
}