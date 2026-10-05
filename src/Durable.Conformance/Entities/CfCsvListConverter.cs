namespace Durable.Conformance
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Durable;

    /// <summary>
    /// Stores a string list as comma-separated text (an empty list as an empty string). Thread safety: stateless.
    /// </summary>
    public class CfCsvListConverter : ValueConverter<List<string>, string>
    {
        /// <summary>
        /// Converts to the stored value.
        /// </summary>
        /// <param name="value">List; null is stored as an empty string.</param>
        /// <returns>Comma-separated text. Never null.</returns>
        public override string ConvertToProvider(List<string> value)
        {
            return string.Join(",", value ?? new List<string>());
        }

        /// <summary>
        /// Converts from the stored value.
        /// </summary>
        /// <param name="value">Comma-separated text; null or empty yields an empty list.</param>
        /// <returns>The list. Never null.</returns>
        public override List<string> ConvertFromProvider(string value)
        {
            if (string.IsNullOrEmpty(value)) return new List<string>();
            return value.Split(',', StringSplitOptions.None).ToList();
        }
    }
}
