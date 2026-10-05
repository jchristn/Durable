namespace Test.Shared
{
    using System;
    using Durable;

    /// <summary>
    /// Custom enum mapping: stores <see cref="RelPriority"/> as "L", "M" or "H".
    /// Thread safety: stateless.
    /// </summary>
    public class PriorityCodeConverter : ValueConverter<RelPriority, string>
    {
        /// <summary>
        /// Converts to the provider value.
        /// </summary>
        /// <param name="value">Priority.</param>
        /// <returns>One-letter code.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown for an undefined priority.</exception>
        public override string ConvertToProvider(RelPriority value)
        {
            switch (value)
            {
                case RelPriority.Low: return "L";
                case RelPriority.Medium: return "M";
                case RelPriority.High: return "H";
                default: throw new ArgumentOutOfRangeException(nameof(value));
            }
        }

        /// <summary>
        /// Converts from the provider value.
        /// </summary>
        /// <param name="value">One-letter code.</param>
        /// <returns>Priority.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown for an unknown code.</exception>
        public override RelPriority ConvertFromProvider(string value)
        {
            switch (value?.Trim())
            {
                case "L": return RelPriority.Low;
                case "M": return RelPriority.Medium;
                case "H": return RelPriority.High;
                default: throw new ArgumentOutOfRangeException(nameof(value), "Unknown priority code '" + value + "'.");
            }
        }
    }
}
