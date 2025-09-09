namespace Durable
{
    using System;
    
    /// <summary>
    /// Represents an exception that is thrown when a concurrency conflict occurs during data operations.
    /// </summary>
    public class ConcurrencyConflictException : Exception
    {
        /// <summary>
        /// Gets or sets the current entity state in the data store at the time of the conflict.
        /// </summary>
        public object? CurrentEntity { get; set; }
        
        /// <summary>
        /// Gets or sets the incoming entity that was attempting to be saved when the conflict occurred.
        /// </summary>
        public object? IncomingEntity { get; set; }
        
        /// <summary>
        /// Gets or sets the original entity state that was used as the baseline for the update operation.
        /// </summary>
        public object? OriginalEntity { get; set; }
        
        /// <summary>
        /// Initializes a new instance of the <see cref="ConcurrencyConflictException"/> class.
        /// </summary>
        public ConcurrencyConflictException() : base()
        {
        }
        
        /// <summary>
        /// Initializes a new instance of the <see cref="ConcurrencyConflictException"/> class with a specified error message.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public ConcurrencyConflictException(string message) : base(message)
        {
        }
        
        /// <summary>
        /// Initializes a new instance of the <see cref="ConcurrencyConflictException"/> class with a specified error message and a reference to the inner exception that is the cause of this exception.
        /// </summary>
        /// <param name="message">The error message that explains the reason for the exception.</param>
        /// <param name="innerException">The exception that is the cause of the current exception, or a null reference if no inner exception is specified.</param>
        public ConcurrencyConflictException(string message, Exception innerException) : base(message, innerException)
        {
        }
        
        /// <summary>
        /// Initializes a new instance of the <see cref="ConcurrencyConflictException"/> class with a specified error message and the entities involved in the conflict.
        /// </summary>
        /// <param name="message">The error message that explains the reason for the exception.</param>
        /// <param name="currentEntity">The current entity state in the data store.</param>
        /// <param name="incomingEntity">The incoming entity that was attempting to be saved.</param>
        /// <param name="originalEntity">The original entity state used as baseline for the update.</param>
        public ConcurrencyConflictException(string message, object currentEntity, object incomingEntity, object originalEntity) 
            : base(message)
        {
            CurrentEntity = currentEntity;
            IncomingEntity = incomingEntity;
            OriginalEntity = originalEntity;
        }
    }
}