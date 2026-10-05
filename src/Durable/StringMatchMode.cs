namespace Durable
{
    /// <summary>
    /// How string comparisons in queries (equality, ordering, <c>IN</c>, Contains/StartsWith/EndsWith, Replace, IndexOf) are evaluated by the backend.
    /// </summary>
    public enum StringMatchMode
    {
        /// <summary>Use the backend's own rules (for SQL, the column or database collation). Results can differ between databases: for example SQL Server's default collation is case-insensitive and MySQL's default is also accent-insensitive. This is the default and matches Durable 0.3.</summary>
        Database,
        /// <summary>Exact, case- and accent-sensitive comparison by character code, like C# <c>StringComparison.Ordinal</c>. SQL dialects apply a binary collation (PostgreSQL <c>"C"</c>, MySQL <c>utf8mb4_bin</c>, SQL Server <c>Latin1_General_100_BIN2</c>, SQLite <c>BINARY</c> plus exact substring functions).</summary>
        Ordinal,
        /// <summary>Case-insensitive but accent-sensitive comparison, like C# <c>StringComparison.OrdinalIgnoreCase</c>: both sides are lower-cased and then compared ordinally. SQLite's built-in lower() folds ASCII letters only.</summary>
        IgnoreCase
    }
}
