namespace Durable.Sql
{
    /// <summary>
    /// One row of <see cref="ISqlDialect.IndexSchemaQuery"/>: a column's membership in an index.
    /// Thread safety: immutable.
    /// </summary>
    internal sealed class IndexColumnRow
    {
        internal string IndexName { get; }

        internal string ColumnName { get; }

        internal bool IsUnique { get; }

        internal long Position { get; }

        internal bool IsIncluded { get; }

        internal IndexColumnRow(string indexName, string columnName, bool isUnique, long position, bool isIncluded)
        {
            IndexName = indexName;
            ColumnName = columnName;
            IsUnique = isUnique;
            Position = position;
            IsIncluded = isIncluded;
        }
    }
}
