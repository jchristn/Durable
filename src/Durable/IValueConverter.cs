namespace Durable
{
    using System;

    /// <summary>
    /// Converts a property value between its model representation and the value stored by the backend.
    /// Implementations must be stateless or thread-safe; one instance is shared per mapped column.
    /// </summary>
    public interface IValueConverter
    {
        /// <summary>
        /// Gets the model (property) type handled by this converter.
        /// </summary>
        Type ModelType { get; }

        /// <summary>
        /// Gets the provider (stored) type produced by this converter.
        /// </summary>
        Type ProviderType { get; }

        /// <summary>
        /// Converts a model value to the stored value. Null in, null out is permitted.
        /// </summary>
        /// <param name="value">Model value; may be null.</param>
        /// <returns>Stored value; may be null.</returns>
        object? ToProvider(object? value);

        /// <summary>
        /// Converts a stored value to the model value. Never receives <see cref="DBNull"/>; null is passed instead.
        /// </summary>
        /// <param name="value">Stored value; may be null.</param>
        /// <returns>Model value; may be null.</returns>
        object? FromProvider(object? value);
    }
}
