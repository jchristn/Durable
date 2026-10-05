namespace Durable
{
    using System;

    /// <summary>
    /// Applies a custom <see cref="IValueConverter"/> to a single mapped property.
    /// The converter takes precedence over all built-in conversions for that column.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class ValueConverterAttribute : Attribute
    {
        /// <summary>
        /// Gets the converter type. Implements <see cref="IValueConverter"/> and has a public parameterless constructor.
        /// </summary>
        public Type ConverterType { get; }

        /// <summary>
        /// Instantiates the attribute.
        /// </summary>
        /// <param name="converterType">Type implementing <see cref="IValueConverter"/> with a public parameterless constructor.</param>
        /// <exception cref="ArgumentNullException">Thrown when converterType is null.</exception>
        /// <exception cref="ArgumentException">Thrown when converterType does not implement <see cref="IValueConverter"/>.</exception>
        public ValueConverterAttribute(Type converterType)
        {
            ArgumentNullException.ThrowIfNull(converterType);
            if (!typeof(IValueConverter).IsAssignableFrom(converterType))
                throw new ArgumentException("Converter type must implement IValueConverter.", nameof(converterType));
            ConverterType = converterType;
        }
    }
}
