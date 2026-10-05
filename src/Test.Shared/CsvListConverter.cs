namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Durable;

    /// <summary>
    /// Stores a list of strings as a single comma-separated string.
    /// Thread safety: stateless.
    /// </summary>
    public class CsvListConverter : ValueConverter<List<string>, string>
    {
        /// <summary>
        /// Converts to the provider value.
        /// </summary>
        /// <param name="value">List; may be empty.</param>
        /// <returns>Comma-separated values.</returns>
        public override string ConvertToProvider(List<string> value)
        {
            return string.Join(",", value ?? new List<string>());
        }

        /// <summary>
        /// Converts from the provider value.
        /// </summary>
        /// <param name="value">Comma-separated values; may be empty.</param>
        /// <returns>The list. Never null.</returns>
        public override List<string> ConvertFromProvider(string value)
        {
            if (string.IsNullOrEmpty(value)) return new List<string>();
            return value.Split(',', StringSplitOptions.None).ToList();
        }
    }
}
