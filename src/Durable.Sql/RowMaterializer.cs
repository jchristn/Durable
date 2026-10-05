namespace Durable.Sql
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Text;
    using Durable;

    /// <summary>
    /// Maps result rows to objects. For each (type, result shape) a typed row reader is compiled once and cached: columns
    /// whose driver type matches (or numerically converts to) the property type are read with <c>GetFieldValue&lt;T&gt;</c>
    /// and assigned without boxing; enums, JSON, converter-backed and mismatched columns go through the
    /// <see cref="IDataTypeConverter"/>. If a provider returns an unexpected value type at runtime, the materializer
    /// switches to the converter path for every column.
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

        private static readonly ConcurrentDictionary<string, RowMaterializer> _Cache = new ConcurrentDictionary<string, RowMaterializer>(StringComparer.Ordinal);
        private static readonly MethodInfo _GetFieldValue = typeof(DbDataReader).GetMethod(nameof(DbDataReader.GetFieldValue))!;
        private static readonly MethodInfo _IsDbNull = typeof(DbDataReader).GetMethod(nameof(DbDataReader.IsDBNull), new[] { typeof(int) })!;
        private static readonly MethodInfo _GetValue = typeof(DbDataReader).GetMethod(nameof(DbDataReader.GetValue), new[] { typeof(int) })!;
        private static readonly MethodInfo _ConvertFromDatabase = typeof(IDataTypeConverter).GetMethod(nameof(IDataTypeConverter.ConvertFromDatabase))!;

        private readonly RowBinding[] _Bindings;
        private readonly Action<DbDataReader, object, IDataTypeConverter> _Compiled;
        private volatile bool _UseFallback;

        #endregion

        #region Constructors-and-Factories

        private RowMaterializer(EntityMetadata metadata, RowBinding[] bindings, Type?[] fieldTypes)
        {
            Metadata = metadata;
            _Bindings = bindings;
            _Compiled = Compile(metadata, bindings, fieldTypes);
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
            StringBuilder key = new StringBuilder(metadata.EntityType.FullName, 64 + fieldCount * 24);
            string[] names = new string[fieldCount];
            Type?[] fieldTypes = new Type?[fieldCount];
            for (int i = 0; i < fieldCount; i++)
            {
                names[i] = reader.GetName(i);
                fieldTypes[i] = SafeFieldType(reader, i);
                key.Append('|').Append(names[i]).Append(':').Append(fieldTypes[i]?.Name);
            }

            return _Cache.GetOrAdd(key.ToString(), _ => Build(metadata, names, fieldTypes));
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
            object instance = Metadata.CreateInstance();
            if (!_UseFallback)
            {
                try
                {
                    _Compiled(reader, instance, converter);
                    return instance;
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

            MaterializeWithConverter(reader, instance, converter);
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

            return new RowMaterializer(metadata, bindings.ToArray(), fieldTypes);
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

        private static Action<DbDataReader, object, IDataTypeConverter> Compile(EntityMetadata metadata, RowBinding[] bindings, Type?[] fieldTypes)
        {
            ParameterExpression reader = Expression.Parameter(typeof(DbDataReader), "reader");
            ParameterExpression instance = Expression.Parameter(typeof(object), "instance");
            ParameterExpression converter = Expression.Parameter(typeof(IDataTypeConverter), "converter");
            ParameterExpression typed = Expression.Variable(metadata.EntityType, "entity");
            List<Expression> body = new List<Expression> { Expression.Assign(typed, Expression.Convert(instance, metadata.EntityType)) };

            foreach (RowBinding binding in bindings)
            {
                ColumnMetadata column = binding.Column;
                PropertyInfo property = column.Property;
                if (property.GetSetMethod(true) == null) continue;

                Expression ordinal = Expression.Constant(binding.Ordinal);
                Expression isNull = Expression.Call(reader, _IsDbNull, ordinal);
                Expression target = Expression.Property(typed, property);
                Expression valueExpression = ReadValue(reader, converter, binding, fieldTypes[binding.Ordinal]);
                Expression assignValue = Expression.Assign(target, valueExpression);

                bool acceptsNull = !column.PropertyType.IsValueType || Nullable.GetUnderlyingType(column.PropertyType) != null;
                Expression whenNull = column.IsNullable && acceptsNull
                    ? Expression.Assign(target, Expression.Default(column.PropertyType))
                    : (Expression)Expression.Empty();
                body.Add(Expression.IfThenElse(isNull, whenNull, assignValue));
            }

            body.Add(Expression.Empty());
            BlockExpression block = Expression.Block(new[] { typed }, body);
            return Expression.Lambda<Action<DbDataReader, object, IDataTypeConverter>>(block, reader, instance, converter).Compile();
        }

        private static Expression ReadValue(ParameterExpression reader, ParameterExpression converter, RowBinding binding, Type? fieldType)
        {
            ColumnMetadata column = binding.Column;
            Type propertyType = column.PropertyType;
            Type target = column.ClrType;
            Expression ordinal = Expression.Constant(binding.Ordinal);

            if (binding.Direct && fieldType != null)
            {
                if (fieldType == target)
                    return Expression.Convert(Expression.Call(reader, _GetFieldValue.MakeGenericMethod(target), ordinal), propertyType);
                if (IsNumeric(fieldType) && IsNumeric(target))
                    return Expression.Convert(Expression.Convert(Expression.Call(reader, _GetFieldValue.MakeGenericMethod(fieldType), ordinal), target), propertyType);
            }

            Expression raw = Expression.Call(reader, _GetValue, ordinal);
            Expression converted = Expression.Call(converter, _ConvertFromDatabase, raw, Expression.Constant(propertyType, typeof(Type)), Expression.Constant(column, typeof(ColumnMetadata)));
            return propertyType.IsValueType && Nullable.GetUnderlyingType(propertyType) == null
                ? Expression.Unbox(converted, propertyType)
                : Expression.Convert(converted, propertyType);
        }

        private static bool IsNumeric(Type type)
        {
            return type == typeof(byte) || type == typeof(sbyte) || type == typeof(short) || type == typeof(ushort)
                || type == typeof(int) || type == typeof(uint) || type == typeof(long) || type == typeof(ulong)
                || type == typeof(float) || type == typeof(double) || type == typeof(decimal);
        }

        #endregion
    }
}
