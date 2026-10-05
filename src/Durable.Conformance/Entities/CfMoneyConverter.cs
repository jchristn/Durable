namespace Durable.Conformance
{
    using Durable;

    /// <summary>
    /// Stores <see cref="CfMoney"/> as its cents value. Thread safety: stateless.
    /// </summary>
    public class CfMoneyConverter : ValueConverter<CfMoney, long>
    {
        /// <summary>
        /// Converts to the stored value.
        /// </summary>
        /// <param name="value">Amount.</param>
        /// <returns>Cents.</returns>
        public override long ConvertToProvider(CfMoney value)
        {
            return value.Cents;
        }

        /// <summary>
        /// Converts from the stored value.
        /// </summary>
        /// <param name="value">Cents.</param>
        /// <returns>Amount.</returns>
        public override CfMoney ConvertFromProvider(long value)
        {
            return new CfMoney(value);
        }
    }
}
