namespace Durable.Conformance
{
    using System;
    using Durable;

    /// <summary>
    /// Stores <see cref="CfGrade"/> as "L", "M" or "H". Thread safety: stateless.
    /// </summary>
    public class CfGradeCodeConverter : ValueConverter<CfGrade, string>
    {
        /// <summary>
        /// Converts to the stored value.
        /// </summary>
        /// <param name="value">Grade.</param>
        /// <returns>The code.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown for an undefined grade.</exception>
        public override string ConvertToProvider(CfGrade value)
        {
            switch (value)
            {
                case CfGrade.Low: return "L";
                case CfGrade.Medium: return "M";
                case CfGrade.High: return "H";
                default: throw new ArgumentOutOfRangeException(nameof(value));
            }
        }

        /// <summary>
        /// Converts from the stored value.
        /// </summary>
        /// <param name="value">The code (surrounding whitespace ignored).</param>
        /// <returns>The grade.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown for an unknown code.</exception>
        public override CfGrade ConvertFromProvider(string value)
        {
            switch (value?.Trim())
            {
                case "L": return CfGrade.Low;
                case "M": return CfGrade.Medium;
                case "H": return CfGrade.High;
                default: throw new ArgumentOutOfRangeException(nameof(value), "Unknown grade code '" + value + "'.");
            }
        }
    }
}
