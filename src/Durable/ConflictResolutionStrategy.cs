namespace Durable
{
    public enum ConflictResolutionStrategy
    {
        ThrowException,
        ClientWins,
        DatabaseWins,
        MergeChanges,
        Custom
    }
}