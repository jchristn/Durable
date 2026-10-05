namespace Durable.Query
{
    /// <summary>
    /// Substring tests in a <see cref="StringMatchNode"/>.
    /// </summary>
    public enum StringMatchKind
    {
        /// <summary>The target contains the pattern.</summary>
        Contains,
        /// <summary>The target starts with the pattern.</summary>
        StartsWith,
        /// <summary>The target ends with the pattern.</summary>
        EndsWith
    }
}
