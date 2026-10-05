namespace Test.Shared
{
    using Durable;

    /// <summary>
    /// Stores <see cref="Money"/> as a BIGINT number of cents.
    /// Thread safety: stateless.
    /// </summary>
    public class MoneyConverter : ValueConverter<Money, long>
    {
        /// <summary>
        /// Converts to the provider value.
        /// </summary>
        /// <param name="value">Money value.</param>
        /// <returns>Cents.</returns>
        public override long ConvertToProvider(Money value)
        {
            return value.Cents;
        }

        /// <summary>
        /// Converts from the provider value.
        /// </summary>
        /// <param name="value">Cents.</param>
        /// <returns>Money value.</returns>
        public override Money ConvertFromProvider(long value)
        {
            return new Money(value);
        }
    }
}
