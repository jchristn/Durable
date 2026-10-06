namespace Test.Aot
{
    /// <summary>
    /// Value object persisted through <see cref="IsbnConverter"/>.
    /// </summary>
    public sealed class Isbn
    {
        public Isbn(string value)
        {
            Value = value;
        }

        public string Value { get; }
    }
}
