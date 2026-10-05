namespace Durable.Sql
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Text;
    using Durable;

    /// <summary>
    /// Maps result rows to objects by ordinal. The column-to-property plan is computed once per (type, result shape) and
    /// cached; per-row work is a compiled setter call per column plus a type check, with conversion only when the driver's
    /// value type differs from the property type.
    /// Thread safety: instances are immutable and thread-safe; the cache is concurrent.
    /// </summary>
    public sealed class RowMaterializer
    {
        #region Public-Members

        /// <summary>
        /// Gets the target metadata. Never null.
        /// </summary>
        public EntityMetadata Metadata { get; }

        /// <summary>
        /// Gets the number of bound columns.
        /// </summary>
        public int BoundColumnCount => _Bindings.Length;

        #endregion

        #region Private-Members

        private static readonly ConcurrentDictionary<string, RowMaterializer> _Cache = new ConcurrentDictionary<string, RowMaterializer>(StringComparer.Ordinal);
        private readonly RowBinding[] _Bindings;

        #endregion

        #region Constructors-and-Factories

        private RowMaterializer(EntityMetadata metadata, RowBinding[] bindings)
        {
            Metadata = metadata;
            _Bindings = bindings;
        }

        /// <summary>
        /// Gets (or builds and caches) the materializer for a type and the current reader's columns.
        /// </summary>
        /// <param name="metadata">Target metadata. Must not be null.</param>
        /// <param name="reader">Reader positioned on a result set. Must not be null.</param>
        /// <returns>The materializer.</returns>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        public static RowMaterializer For(EntityMetadata metadata, DbDataReader reader)
        {
            ArgumentNullException.ThrowIfNull(metadata);
            ArgumentNullException.ThrowIfNull(reader);

            int fieldCount = reader.FieldCount;
            StringBuilder key = new StringBuilder(metadata.EntityType.FullName, 64 + fieldCount * 12);
            string[] names = new string[fieldCount];
            for (int i = 0; i < fieldCount; i++)
            {
                names[i] = reader.GetName(i);
                key.Append('|').Append(names[i]);
            }

            return _Cache.GetOrAdd(key.ToString(), _ => Build(metadata, names));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Materializes the current row.
        /// </summary>
        /// <param name="reader">Reader positioned on a row. Must not be null.</param>
        /// <param name="converter">Converter for values whose type differs from the property type. Must not be null.</param>
        /// <returns>The new object.</returns>
        public object Materialize(DbDataReader reader, IDataTypeConverter converter)
        {
            object instance = Metadata.CreateInstance();
            RowBinding[] bindings = _Bindings;
            for (int i = 0; i < bindings.Length; i++)
            {
                RowBinding binding = bindings[i];
                if (reader.IsDBNull(binding.Ordinal))
                {
                    if (binding.Column.IsNullable) binding.Column.SetValue(instance, null);
                    continue;
                }

                object raw = reader.GetValue(binding.Ordinal);
                object? value = binding.Direct && raw.GetType() == binding.Column.ClrType
                    ? raw
                    : converter.ConvertFromDatabase(raw, binding.Column.PropertyType, binding.Column);
                binding.Column.SetValue(instance, value);
            }

            return instance;
        }

        /// <summary>
        /// Reads the value of a mapped column from the current row, converted to the property type.
        /// </summary>
        /// <param name="reader">Reader positioned on a row. Must not be null.</param>
        /// <param name="column">Column. Must not be null.</param>
        /// <param name="converter">Converter. Must not be null.</param>
        /// <returns>The value, or null when the column is absent or null.</returns>
        public object? ReadColumn(DbDataReader reader, ColumnMetadata column, IDataTypeConverter converter)
        {
            foreach (RowBinding binding in _Bindings)
            {
                if (!ReferenceEquals(binding.Column, column)) continue;
                if (reader.IsDBNull(binding.Ordinal)) return null;
                return converter.ConvertFromDatabase(reader.GetValue(binding.Ordinal), column.PropertyType, column);
            }

            return null;
        }

        #endregion

        #region Private-Methods

        private static RowMaterializer Build(EntityMetadata metadata, string[] names)
        {
            List<RowBinding> bindings = new List<RowBinding>(names.Length);
            HashSet<ColumnMetadata> bound = new HashSet<ColumnMetadata>();
            for (int i = 0; i < names.Length; i++)
            {
                ColumnMetadata? column = metadata.MatchResultColumn(names[i]);
                if (column == null || !bound.Add(column)) continue;
                bindings.Add(new RowBinding(i, column, column.Converter == null && !column.IsEnum && !column.IsJson));
            }

            return new RowMaterializer(metadata, bindings.ToArray());
        }

        #endregion
    }
}
