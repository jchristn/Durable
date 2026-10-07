namespace Durable.DuckDb
{
    /// <summary>
    /// How one bulk insert maps prepared rows onto a table's columns for the DuckDB Appender (which appends every column
    /// in table order). Sources: an index into the row values, -1 for NULL, -2 for a value pre-generated from the
    /// column's sequence default.
    /// Thread safety: not thread-safe; used by one bulk insert.
    /// </summary>
    internal sealed class AppenderPlan
    {
        internal AppenderPlan(string schema, string table, int[] sources, object[][] rows, object?[][] generated)
        {
            Schema = schema;
            Table = table;
            Sources = sources;
            Rows = rows;
            Generated = generated;
        }

        internal string Schema { get; }

        internal string Table { get; }

        internal int[] Sources { get; }

        internal object[][] Rows { get; }

        internal object?[][] Generated { get; }
    }
}
