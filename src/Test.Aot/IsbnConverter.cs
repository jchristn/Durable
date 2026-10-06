namespace Test.Aot
{
    using Durable;

    /// <summary>
    /// Stores <see cref="Isbn"/> as "ISBN:value" text.
    /// </summary>
    public sealed class IsbnConverter : ValueConverter<Isbn, string>
    {
        public override string ConvertToProvider(Isbn value)
        {
            return "ISBN:" + value.Value;
        }

        public override Isbn ConvertFromProvider(string value)
        {
            return new Isbn(value.StartsWith("ISBN:", System.StringComparison.Ordinal) ? value.Substring(5) : value);
        }
    }
}
