namespace Durable
{
    using System;
    
    /// <summary>
    /// Exception thrown when an optimistic concurrency conflict is detected during entity updates.
    /// </summary>
    public class OptimisticConcurrencyException : Exception
    {
        #region Public-Members

        /// <summary>
        /// Gets the entity that caused the concurrency conflict.
        /// </summary>
        public object Entity { get; init; } = null!;
        
        /// <summary>
        /// Gets the expected version value that was used in the update operation.
        /// </summary>
        public object ExpectedVersion { get; init; } = null!;
        
        /// <summary>
        /// Gets the actual version value found in the database.
        /// </summary>
        public object ActualVersion { get; init; } = null!;

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="OptimisticConcurrencyException"/> class with a specified error message.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public OptimisticConcurrencyException(string message) 
            : base(message)
        {
        }
        
        /// <summary>
        /// Initializes a new instance of the <see cref="OptimisticConcurrencyException"/> class with a specified error message and inner exception.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        /// <param name="innerException">The exception that is the cause of the current exception.</param>
        public OptimisticConcurrencyException(string message, Exception innerException) 
            : base(message, innerException)
        {
        }
        
        /// <summary>
        /// Initializes a new instance of the <see cref="OptimisticConcurrencyException"/> class with entity and version information.
        /// </summary>
        /// <param name="entity">The entity that caused the conflict.</param>
        /// <param name="expectedVersion">The expected version value.</param>
        /// <param name="actualVersion">The actual version value found in the database.</param>
        public OptimisticConcurrencyException(object entity, object expectedVersion, object actualVersion)
            : base($"Optimistic concurrency conflict detected. Expected version: {expectedVersion}, Actual version: {actualVersion}")
        {
            Entity = entity;
            ExpectedVersion = expectedVersion;
            ActualVersion = actualVersion;
        }
        
        /// <summary>
        /// Initializes a new instance of the <see cref="OptimisticConcurrencyException"/> class with a custom message and entity version information.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        /// <param name="entity">The entity that caused the conflict.</param>
        /// <param name="expectedVersion">The expected version value.</param>
        /// <param name="actualVersion">The actual version value found in the database.</param>
        public OptimisticConcurrencyException(string message, object entity, object expectedVersion, object actualVersion)
            : base(message)
        {
            Entity = entity;
            ExpectedVersion = expectedVersion;
            ActualVersion = actualVersion;
        }

        #endregion

        #region Public-Methods

        #endregion

        #region Private-Methods

        #endregion
    }
}