namespace Durable.MySql
{
    /// <summary>
    /// Specifies the search mode for MySQL full-text search operations.
    /// </summary>
    public enum FullTextSearchMode
    {
        /// <summary>
        /// Natural language search mode (default). Interprets the search string as a phrase in natural human language.
        /// Words are stemmed and stop words are ignored. Results are ranked by relevance.
        /// </summary>
        Natural,

        /// <summary>
        /// Natural language search mode with query expansion. First performs a natural language search,
        /// then runs a second search that includes the most relevant documents from the first search.
        /// Useful for finding related documents even if they don't contain the exact search terms.
        /// </summary>
        NaturalWithQueryExpansion,

        /// <summary>
        /// Boolean search mode. Allows special operators in the search string:
        /// + (must contain), - (must not contain), * (wildcard), "" (exact phrase),
        /// &gt; (increase relevance), &lt; (decrease relevance), () (grouping), ~ (negation).
        /// Results are not automatically ranked by relevance.
        /// </summary>
        Boolean,

        /// <summary>
        /// Boolean search mode with query expansion. Combines boolean operators with query expansion.
        /// First performs a boolean search, then expands the results with related documents.
        /// </summary>
        BooleanWithQueryExpansion
    }
}
