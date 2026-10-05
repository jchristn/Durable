namespace Durable.Query
{
    /// <summary>
    /// Unary string tests in a <see cref="StringTestNode"/>.
    /// </summary>
    public enum StringTestKind
    {
        /// <summary><c>string.IsNullOrEmpty</c>.</summary>
        IsNullOrEmpty,
        /// <summary><c>string.IsNullOrWhiteSpace</c>.</summary>
        IsNullOrWhiteSpace
    }
}
