namespace Durable.Sql
{
    using Durable;

    /// <summary>
    /// One result-set ordinal bound to a mapped column. Internal to <see cref="RowMaterializer"/>.
    /// </summary>
    internal sealed class RowBinding
    {
        internal RowBinding(int ordinal, ColumnMetadata column, bool direct)
        {
            Ordinal = ordinal;
            Column = column;
            Direct = direct;
        }

        internal int Ordinal { get; }

        internal ColumnMetadata Column { get; }

        internal bool Direct { get; }
    }
}
