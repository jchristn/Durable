using System;

namespace Durable
{
    public class OptimisticConcurrencyException : Exception
    {
        public object Entity { get; init; } = null!;
        public object ExpectedVersion { get; init; } = null!;
        public object ActualVersion { get; init; } = null!;
        
        public OptimisticConcurrencyException(string message) 
            : base(message)
        {
        }
        
        public OptimisticConcurrencyException(string message, Exception innerException) 
            : base(message, innerException)
        {
        }
        
        public OptimisticConcurrencyException(object entity, object expectedVersion, object actualVersion)
            : base($"Optimistic concurrency conflict detected. Expected version: {expectedVersion}, Actual version: {actualVersion}")
        {
            Entity = entity;
            ExpectedVersion = expectedVersion;
            ActualVersion = actualVersion;
        }
        
        public OptimisticConcurrencyException(string message, object entity, object expectedVersion, object actualVersion)
            : base(message)
        {
            Entity = entity;
            ExpectedVersion = expectedVersion;
            ActualVersion = actualVersion;
        }
    }
}