namespace Test.Shared
{
    using Durable;

    /// <summary>
    /// Stores a <see cref="RegCode"/> as upper-case text prefixed with "C-".
    /// </summary>
    public class RegCodeConverter : ValueConverter<RegCode, string>
    {
        /// <inheritdoc />
        public override string ConvertToProvider(RegCode value)
        {
            return "C-" + value.Value.ToUpperInvariant();
        }

        /// <inheritdoc />
        public override RegCode ConvertFromProvider(string value)
        {
            string text = value ?? string.Empty;
            return new RegCode(text.StartsWith("C-", System.StringComparison.Ordinal) ? text.Substring(2).ToLowerInvariant() : text);
        }
    }
}
