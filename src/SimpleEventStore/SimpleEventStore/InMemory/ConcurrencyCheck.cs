namespace SimpleEventStore.InMemory
{
    public enum ConcurrencyCheck
    {
        ThrowExceptionOnConflict,
        AllowMissingAndDuplicatedEventNumbersAndAppendRegardless
    }
}
