namespace Durable.InMemory
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Durable;
    using Durable.Query;

    /// <summary>
    /// Evaluates <see cref="QueryNode"/> trees against stored rows of an <see cref="InMemoryDatabaseState"/> with the C#
    /// semantics of <see cref="QueryEvaluator{TRow}"/>. Values are compared in their stored form (enum names, converter
    /// provider values, JSON text): parameter values are converted with <see cref="InMemoryValueConverter.ToStored"/>,
    /// exactly as a database compares column values with converted parameters. Related rows are looked up by primary key
    /// when the navigation targets the key, otherwise by scanning the related table.
    /// Thread safety: not thread-safe; create one per operation. The state it reads is immutable.
    /// </summary>
    internal sealed class InMemoryQueryEvaluator : QueryEvaluator<InMemoryRow>
    {
        private readonly InMemoryDatabaseState _State;
        private readonly InMemoryValueConverter _Values;

        /// <summary>
        /// Instantiates an evaluator.
        /// </summary>
        /// <param name="state">State related rows are read from. Must not be null.</param>
        /// <param name="values">Value converter. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        public InMemoryQueryEvaluator(InMemoryDatabaseState state, InMemoryValueConverter values)
        {
            _State = state ?? throw new ArgumentNullException(nameof(state));
            _Values = values ?? throw new ArgumentNullException(nameof(values));
        }

        /// <inheritdoc />
        protected override object? GetValue(EntityMetadata metadata, InMemoryRow row, ColumnMetadata column)
        {
            return row.Values[InMemoryTableSchema.For(metadata).Ordinal(column)];
        }

        /// <inheritdoc />
        protected override IEnumerable<InMemoryRow> FindRows(EntityMetadata metadata, ColumnMetadata column, object key)
        {
            InMemoryTable table = _State.Table(metadata);
            if (metadata.KeyColumns.Count == 1 && ReferenceEquals(metadata.KeyColumns[0], column))
            {
                InMemoryRow? row = table.Find(new InMemoryRowKey(new[] { key }));
                return row == null ? Array.Empty<InMemoryRow>() : new[] { row };
            }

            int ordinal = InMemoryTableSchema.For(metadata).Ordinal(column);
            return table.Rows.Where(row => QueryValueComparer.Ordinal.Equals(row.Values[ordinal], key));
        }

        /// <inheritdoc />
        protected override object? NormalizeValue(ColumnMetadata column, object? value)
        {
            return _Values.ToStored(column, value);
        }
    }
}
