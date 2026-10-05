namespace Durable.Sql
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Threading;
    using Durable;

    /// <summary>
    /// Maps result rows to objects. For each (type, result shape) a typed row reader is compiled once and cached: it
    /// constructs the instance inline and reads columns whose driver type matches (or numerically converts to) the property
    /// type with the reader's typed getters, without boxing. Enums, booleans, GUIDs and dates that the stock
    /// <see cref="DataTypeConverter"/> converts cheaply are converted inline with identical rules; JSON, converter-backed
    /// and other mismatched columns go through the <see cref="IDataTypeConverter"/>. If a provider returns an unexpected
    /// value type at runtime, the materializer switches to the converter path for every column.
    /// Thread safety: instances are thread-safe; the cache is concurrent.
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

        private static readonly ConcurrentDictionary<EntityMetadata, RowMaterializer[]> _Shapes = new ConcurrentDictionary<EntityMetadata, RowMaterializer[]>();
        private static readonly object _ShapesLock = new object();

        private readonly string[] _Names;
        private readonly Type?[] _FieldTypes;
        private readonly RowBinding[] _Bindings;
        private readonly Delegate?[] _TypedReaders = new Delegate?[2];
        private readonly Func<DbDataReader, IDataTypeConverter, object>?[] _ObjectReaders = new Func<DbDataReader, IDataTypeConverter, object>?[2];
        private volatile bool _UseFallback;

        #endregion

        #region Constructors-and-Factories

        private RowMaterializer(EntityMetadata metadata, string[] names, Type?[] fieldTypes, RowBinding[] bindings)
        {
            Metadata = metadata;
            _Names = names;
            _FieldTypes = fieldTypes;
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
            if (_Shapes.TryGetValue(metadata, out RowMaterializer[]? shapes))
            {
                foreach (RowMaterializer candidate in shapes)
                {
                    if (candidate.Matches(reader, fieldCount)) return candidate;
                }
            }

            string[] names = new string[fieldCount];
            Type?[] fieldTypes = new Type?[fieldCount];
            for (int i = 0; i < fieldCount; i++)
            {
                names[i] = reader.GetName(i);
                fieldTypes[i] = SafeFieldType(reader, i);
            }

            lock (_ShapesLock)
            {
                RowMaterializer[] current = _Shapes.TryGetValue(metadata, out RowMaterializer[]? existing) ? existing : Array.Empty<RowMaterializer>();
                foreach (RowMaterializer candidate in current)
                {
                    if (candidate.Matches(names, fieldTypes)) return candidate;
                }

                RowMaterializer built = Build(metadata, names, fieldTypes);
                RowMaterializer[] updated = new RowMaterializer[current.Length + 1];
                Array.Copy(current, updated, current.Length);
                updated[current.Length] = built;
                _Shapes[metadata] = updated;
                return built;
            }
        }

        /// <summary>
        /// Creates a row mapper for <typeparamref name="T"/> that resolves the materializer from the first row's result shape
        /// and then reads every row with a compiled, typed reader. Create one mapper per command; the mapper caches the
        /// result shape of the first reader it sees.
        /// </summary>
        /// <typeparam name="T">Result type; <paramref name="metadata"/> must describe it or a type assignable to it.</typeparam>
        /// <param name="metadata">Target metadata. Must not be null.</param>
        /// <param name="converter">Converter for values that need conversion. Must not be null.</param>
        /// <returns>The mapper. Not thread-safe; use one per command.</returns>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        /// <exception cref="ArgumentException">Thrown when the metadata type is not assignable to <typeparamref name="T"/>.</exception>
        public static Func<DbDataReader, T> CreateMapper<T>(EntityMetadata metadata, IDataTypeConverter converter)
        {
            ArgumentNullException.ThrowIfNull(metadata);
            ArgumentNullException.ThrowIfNull(converter);
            if (!typeof(T).IsAssignableFrom(metadata.EntityType))
                throw new ArgumentException("Metadata for " + metadata.EntityType.Name + " cannot produce " + typeof(T).Name + ".", nameof(metadata));
            return new TypedRowMapper<T>(metadata, converter, RowReaderCompiler.CanInline(converter)).Map;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Materializes the current row.
        /// </summary>
        /// <param name="reader">Reader positioned on a row. Must not be null.</param>
        /// <param name="converter">Converter for values that need conversion. Must not be null.</param>
        /// <returns>The new object.</returns>
        public object Materialize(DbDataReader reader, IDataTypeConverter converter)
        {
            return Materialize(reader, converter, RowReaderCompiler.CanInline(converter));
        }

        /// <summary>
        /// Materializes the current row as <typeparamref name="T"/> without casting through <see cref="object"/> when
        /// <typeparamref name="T"/> is the metadata type.
        /// </summary>
        /// <typeparam name="T">Result type; must be the metadata type or assignable from it.</typeparam>
        /// <param name="reader">Reader positioned on a row. Must not be null.</param>
        /// <param name="converter">Converter for values that need conversion. Must not be null.</param>
        /// <returns>The new object.</returns>
        public T Materialize<T>(DbDataReader reader, IDataTypeConverter converter)
        {
            bool inline = RowReaderCompiler.CanInline(converter);
            if (typeof(T) != Metadata.EntityType) return (T)Materialize(reader, converter, inline);
            return Read(reader, converter, GetTypedReader<T>(inline));
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

        internal object Materialize(DbDataReader reader, IDataTypeConverter converter, bool inline)
        {
            int index = inline ? 1 : 0;
            Func<DbDataReader, IDataTypeConverter, object>? compiled = _ObjectReaders[index];
            if (compiled == null)
            {
                Delegate built = Metadata.EntityType.IsValueType
                    ? RowReaderCompiler.Compile(Metadata, _Bindings, _FieldTypes, inline, typeof(object))
                    : GetTypedReader(inline);
                Interlocked.CompareExchange(ref _ObjectReaders[index], (Func<DbDataReader, IDataTypeConverter, object>)built, null);
                compiled = _ObjectReaders[index]!;
            }

            return Read(reader, converter, compiled);
        }

        internal Func<DbDataReader, IDataTypeConverter, T> GetTypedReader<T>(bool inline)
        {
            return (Func<DbDataReader, IDataTypeConverter, T>)GetTypedReader(inline);
        }

        internal T Read<T>(DbDataReader reader, IDataTypeConverter converter, Func<DbDataReader, IDataTypeConverter, T> compiled)
        {
            if (!_UseFallback)
            {
                try
                {
                    return compiled(reader, converter);
                }
                catch (InvalidCastException)
                {
                    _UseFallback = true;
                }
                catch (FormatException)
                {
                    _UseFallback = true;
                }
                catch (OverflowException)
                {
                    _UseFallback = true;
                }
            }

            object instance = Metadata.CreateInstance();
            MaterializeWithConverter(reader, instance, converter);
            return (T)instance;
        }

        private Delegate GetTypedReader(bool inline)
        {
            int index = inline ? 1 : 0;
            Delegate? compiled = _TypedReaders[index];
            if (compiled != null) return compiled;
            Delegate built = RowReaderCompiler.Compile(Metadata, _Bindings, _FieldTypes, inline, Metadata.EntityType);
            return Interlocked.CompareExchange(ref _TypedReaders[index], built, null) ?? built;
        }

        private bool Matches(DbDataReader reader, int fieldCount)
        {
            if (_Names.Length != fieldCount) return false;
            for (int i = 0; i < fieldCount; i++)
            {
                if (_FieldTypes[i] != SafeFieldType(reader, i)) return false;
                if (!string.Equals(_Names[i], reader.GetName(i), StringComparison.Ordinal)) return false;
            }

            return true;
        }

        private bool Matches(string[] names, Type?[] fieldTypes)
        {
            if (_Names.Length != names.Length) return false;
            for (int i = 0; i < names.Length; i++)
            {
                if (_FieldTypes[i] != fieldTypes[i]) return false;
                if (!string.Equals(_Names[i], names[i], StringComparison.Ordinal)) return false;
            }

            return true;
        }

        private static Type? SafeFieldType(DbDataReader reader, int ordinal)
        {
            try
            {
                return reader.GetFieldType(ordinal);
            }
            catch (InvalidOperationException)
            {
                return null;
            }
            catch (NotSupportedException)
            {
                return null;
            }
        }

        private static RowMaterializer Build(EntityMetadata metadata, string[] names, Type?[] fieldTypes)
        {
            List<RowBinding> bindings = new List<RowBinding>(names.Length);
            HashSet<ColumnMetadata> bound = new HashSet<ColumnMetadata>();
            for (int i = 0; i < names.Length; i++)
            {
                ColumnMetadata? column = metadata.MatchResultColumn(names[i]);
                if (column == null || !bound.Add(column)) continue;
                bindings.Add(new RowBinding(i, column, column.Converter == null && !column.IsEnum && !column.IsJson));
            }

            return new RowMaterializer(metadata, names, fieldTypes, bindings.ToArray());
        }

        private void MaterializeWithConverter(DbDataReader reader, object instance, IDataTypeConverter converter)
        {
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
        }

        #endregion
    }
}
