using System;

namespace Durable
{
    public class ConcurrencyConflictException : Exception
    {
        public object? CurrentEntity { get; set; }
        public object? IncomingEntity { get; set; }
        public object? OriginalEntity { get; set; }
        
        public ConcurrencyConflictException() : base()
        {
        }
        
        public ConcurrencyConflictException(string message) : base(message)
        {
        }
        
        public ConcurrencyConflictException(string message, Exception innerException) : base(message, innerException)
        {
        }
        
        public ConcurrencyConflictException(string message, object currentEntity, object incomingEntity, object originalEntity) 
            : base(message)
        {
            CurrentEntity = currentEntity;
            IncomingEntity = incomingEntity;
            OriginalEntity = originalEntity;
        }
    }
}