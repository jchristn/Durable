namespace Durable
{
    using System;

    /// <summary>
    /// Strongly-typed base class for <see cref="IValueConverter"/> implementations.
    /// Derived classes must have a public parameterless constructor when used with <see cref="ValueConverterAttribute"/>.
    /// </summary>
    /// <typeparam name="TModel">Property type.</typeparam>
    /// <typeparam name="TProvider">Stored type.</typeparam>
    public abstract class ValueConverter<TModel, TProvider> : IValueConverter
    {
        /// <inheritdoc />
        public Type ModelType => typeof(TModel);

        /// <inheritdoc />
        public Type ProviderType => typeof(TProvider);

        /// <summary>
        /// Converts a model value to the stored value.
        /// </summary>
        /// <param name="value">Model value.</param>
        /// <returns>Stored value.</returns>
        public abstract TProvider ConvertToProvider(TModel value);

        /// <summary>
        /// Converts a stored value to the model value.
        /// </summary>
        /// <param name="value">Stored value.</param>
        /// <returns>Model value.</returns>
        public abstract TModel ConvertFromProvider(TProvider value);

        /// <inheritdoc />
        public object? ToProvider(object? value)
        {
            if (value == null) return null;
            return ConvertToProvider((TModel)value);
        }

        /// <inheritdoc />
        public object? FromProvider(object? value)
        {
            if (value == null) return null;
            if (value is TProvider typed) return ConvertFromProvider(typed);
            return ConvertFromProvider((TProvider)Convert.ChangeType(value, Nullable.GetUnderlyingType(typeof(TProvider)) ?? typeof(TProvider), System.Globalization.CultureInfo.InvariantCulture));
        }
    }
}
