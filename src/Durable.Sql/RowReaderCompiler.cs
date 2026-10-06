namespace Durable.Sql
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Diagnostics.CodeAnalysis;
    using System.Globalization;
    using System.Linq.Expressions;
    using System.Reflection;
    using Durable;
    using Durable.Sql.Helpers;

    /// <summary>
    /// Compiles row readers for <see cref="RowMaterializer"/>: a delegate that constructs the instance inline and assigns
    /// every bound column with the reader's typed getters. Internal to <see cref="RowMaterializer"/>.
    /// When the converter is a stock <see cref="DataTypeConverter"/> (its read conversion is not overridden), cheap
    /// conversions are inlined with exactly the converter's semantics; everything else calls the converter.
    /// Only used when <see cref="System.Runtime.CompilerServices.RuntimeFeature.IsDynamicCodeSupported"/> is true; under
    /// Native AOT, <see cref="RowMaterializer"/> reads rows with cached reflection accessors instead.
    /// Thread safety: stateless apart from a concurrent cache; safe for concurrent use.
    /// </summary>
    internal static class RowReaderCompiler
    {
        private static readonly ConcurrentDictionary<Type, bool> _InlineableConverters = new ConcurrentDictionary<Type, bool>();
        private static readonly MethodInfo _GetFieldValue = typeof(DbDataReader).GetMethod(nameof(DbDataReader.GetFieldValue))!;
        private static readonly MethodInfo _IsDbNull = typeof(DbDataReader).GetMethod(nameof(DbDataReader.IsDBNull), new[] { typeof(int) })!;
        private static readonly MethodInfo _GetValue = typeof(DbDataReader).GetMethod(nameof(DbDataReader.GetValue), new[] { typeof(int) })!;
        private static readonly MethodInfo _GetFieldType = GetFieldTypeMethod();
        private static readonly MethodInfo _GetString = typeof(DbDataReader).GetMethod(nameof(DbDataReader.GetString), new[] { typeof(int) })!;
        private static readonly MethodInfo _ConvertFromDatabase = typeof(IDataTypeConverter).GetMethod(nameof(IDataTypeConverter.ConvertFromDatabase))!;
        private static readonly MethodInfo _CreateInstance = typeof(EntityMetadata).GetMethod(nameof(EntityMetadata.CreateInstance))!;
        private static readonly MethodInfo _EnumParse = typeof(Enum).GetMethod(nameof(Enum.Parse), 1, new[] { typeof(string), typeof(bool) })!;
        private static readonly MethodInfo _GuidParse = typeof(Guid).GetMethod(nameof(Guid.Parse), new[] { typeof(string) })!;
        private static readonly MethodInfo _ParseDateTime = typeof(DateTimeParser).GetMethod(nameof(DateTimeParser.ParseString), new[] { typeof(string) })!;
        private static readonly MethodInfo _ParseDateTimeOffset = typeof(DateTimeOffsetParser).GetMethod(nameof(DateTimeOffsetParser.ParseString), new[] { typeof(string) })!;
        private static readonly MethodInfo _TimeSpanFromTicks = typeof(TimeSpan).GetMethod(nameof(TimeSpan.FromTicks), new[] { typeof(long) })!;
        private static readonly MethodInfo _DateOnlyFromDateTime = typeof(DateOnly).GetMethod(nameof(DateOnly.FromDateTime), new[] { typeof(DateTime) })!;
        private static readonly MethodInfo _TimeOnlyFromTimeSpan = typeof(TimeOnly).GetMethod(nameof(TimeOnly.FromTimeSpan), new[] { typeof(TimeSpan) })!;

        private static readonly Dictionary<Type, MethodInfo> _TypedGetters = new Dictionary<Type, MethodInfo>
        {
            { typeof(bool), typeof(DbDataReader).GetMethod(nameof(DbDataReader.GetBoolean), new[] { typeof(int) })! },
            { typeof(byte), typeof(DbDataReader).GetMethod(nameof(DbDataReader.GetByte), new[] { typeof(int) })! },
            { typeof(short), typeof(DbDataReader).GetMethod(nameof(DbDataReader.GetInt16), new[] { typeof(int) })! },
            { typeof(int), typeof(DbDataReader).GetMethod(nameof(DbDataReader.GetInt32), new[] { typeof(int) })! },
            { typeof(long), typeof(DbDataReader).GetMethod(nameof(DbDataReader.GetInt64), new[] { typeof(int) })! },
            { typeof(float), typeof(DbDataReader).GetMethod(nameof(DbDataReader.GetFloat), new[] { typeof(int) })! },
            { typeof(double), typeof(DbDataReader).GetMethod(nameof(DbDataReader.GetDouble), new[] { typeof(int) })! },
            { typeof(decimal), typeof(DbDataReader).GetMethod(nameof(DbDataReader.GetDecimal), new[] { typeof(int) })! },
            { typeof(DateTime), typeof(DbDataReader).GetMethod(nameof(DbDataReader.GetDateTime), new[] { typeof(int) })! },
            { typeof(Guid), typeof(DbDataReader).GetMethod(nameof(DbDataReader.GetGuid), new[] { typeof(int) })! },
            { typeof(string), typeof(DbDataReader).GetMethod(nameof(DbDataReader.GetString), new[] { typeof(int) })! }
        };

        /// <summary>
        /// Returns whether conversions for this converter may be inlined: true only for a <see cref="DataTypeConverter"/>
        /// whose read path (<c>ConvertFromDatabase</c> and <c>FromDatabaseCore</c>) is the stock implementation.
        /// </summary>
        /// <param name="converter">Converter. Must not be null.</param>
        /// <returns>True when inlining is safe.</returns>
        internal static bool CanInline(IDataTypeConverter converter)
        {
            if (converter is not DataTypeConverter) return false;
            return _InlineableConverters.GetOrAdd(converter.GetType(), IsStockReadPath);
        }

        /// <summary>
        /// Compiles a reader returning the entity typed as <paramref name="resultType"/>.
        /// </summary>
        /// <param name="metadata">Entity metadata.</param>
        /// <param name="bindings">Bound columns.</param>
        /// <param name="fieldTypes">Driver field types per ordinal (null when unknown).</param>
        /// <param name="inline">Whether converter conversions may be inlined.</param>
        /// <param name="resultType">Delegate return type: the entity type or <see cref="object"/>.</param>
        /// <returns>A <c>Func&lt;DbDataReader, IDataTypeConverter, TResult&gt;</c>.</returns>
        [RequiresDynamicCode("Compiles expression trees with generic instantiations chosen at run time; RowMaterializer only calls this when RuntimeFeature.IsDynamicCodeSupported is true and otherwise materializes rows with cached reflection accessors.")]
        internal static Delegate Compile(EntityMetadata metadata, RowBinding[] bindings, Type?[] fieldTypes, bool inline, Type resultType)
        {
            Type entityType = metadata.EntityType;
            ParameterExpression reader = Expression.Parameter(typeof(DbDataReader), "reader");
            ParameterExpression converter = Expression.Parameter(typeof(IDataTypeConverter), "converter");
            ParameterExpression entity = Expression.Variable(entityType, "entity");
            List<Expression> body = new List<Expression> { Expression.Assign(entity, NewInstance(metadata)) };

            foreach (RowBinding binding in bindings)
            {
                ColumnMetadata column = binding.Column;
                PropertyInfo property = column.Property;
                if (property.GetSetMethod(true) == null) continue;

                Expression ordinal = Expression.Constant(binding.Ordinal);
                Expression isNull = Expression.Call(reader, _IsDbNull, ordinal);
                Expression target = Expression.Property(entity, property);
                Expression assignValue = Expression.Assign(target, ReadValue(reader, converter, binding, fieldTypes[binding.Ordinal], inline));

                bool acceptsNull = !column.PropertyType.IsValueType || Nullable.GetUnderlyingType(column.PropertyType) != null;
                if (column.IsNullable && acceptsNull)
                    body.Add(Expression.IfThenElse(isNull, Expression.Assign(target, Expression.Default(column.PropertyType)), assignValue));
                else
                    body.Add(Expression.IfThen(Expression.Not(isNull), assignValue));
            }

            body.Add(resultType == entityType ? entity : Expression.Convert(entity, resultType));
            BlockExpression block = Expression.Block(resultType, new[] { entity }, body);
            Type delegateType = typeof(Func<,,>).MakeGenericType(typeof(DbDataReader), typeof(IDataTypeConverter), resultType);
            return Expression.Lambda(delegateType, block, reader, converter).Compile();
        }

        [UnconditionalSuppressMessage("Trimming", "IL2111", Justification = "GetFieldType is only called from compiled readers to compare the driver type with a Type constant; the returned Type is never used for reflection, so its DynamicallyAccessedMembers requirement does not apply.")]
        private static MethodInfo GetFieldTypeMethod()
        {
            return typeof(DbDataReader).GetMethod(nameof(DbDataReader.GetFieldType), new[] { typeof(int) })!;
        }

        [UnconditionalSuppressMessage("Trimming", "IL2070", Justification = "Detects whether a converter overrides FromDatabaseCore. Both methods are called virtually, so trimming keeps them and their metadata; if a lookup still finds nothing the method returns false, which only disables the inlining optimization.")]
        private static bool IsStockReadPath(Type type)
        {
            MethodInfo? core = type.GetMethod(
                "FromDatabaseCore",
                BindingFlags.Instance | BindingFlags.NonPublic,
                null,
                new[] { typeof(object), typeof(Type), typeof(Type), typeof(ColumnMetadata) },
                null);
            if (core == null || core.DeclaringType != typeof(DataTypeConverter)) return false;

            InterfaceMapping map = type.GetInterfaceMap(typeof(IDataTypeConverter));
            for (int i = 0; i < map.InterfaceMethods.Length; i++)
            {
                if (map.InterfaceMethods[i].Name != nameof(IDataTypeConverter.ConvertFromDatabase)) continue;
                if (map.TargetMethods[i].DeclaringType != typeof(DataTypeConverter)) return false;
            }

            return true;
        }

        private static Expression NewInstance(EntityMetadata metadata)
        {
            Type type = metadata.EntityType;
            if (type.IsValueType) return Expression.Default(type);
            ConstructorInfo? ctor = type.GetConstructor(Type.EmptyTypes)
                ?? type.GetConstructor(BindingFlags.Instance | BindingFlags.NonPublic, null, Type.EmptyTypes, null);
            if (ctor != null) return Expression.New(ctor);
            return Expression.Convert(Expression.Call(Expression.Constant(metadata), _CreateInstance), type);
        }

        [RequiresDynamicCode("Compiles expression trees with generic instantiations chosen at run time; RowMaterializer only calls this when RuntimeFeature.IsDynamicCodeSupported is true and otherwise materializes rows with cached reflection accessors.")]
        private static Expression TypedGet(ParameterExpression reader, Type fieldType, Expression ordinal)
        {
            if (_TypedGetters.TryGetValue(fieldType, out MethodInfo? getter)) return Expression.Call(reader, getter, ordinal);
            return Expression.Call(reader, _GetFieldValue.MakeGenericMethod(fieldType), ordinal);
        }

        [RequiresDynamicCode("Compiles expression trees with generic instantiations chosen at run time; RowMaterializer only calls this when RuntimeFeature.IsDynamicCodeSupported is true and otherwise materializes rows with cached reflection accessors.")]
        private static Expression ReadValue(ParameterExpression reader, ParameterExpression converter, RowBinding binding, Type? fieldType, bool inline)
        {
            ColumnMetadata column = binding.Column;
            Type propertyType = column.PropertyType;
            Type target = column.ClrType;
            Expression ordinal = Expression.Constant(binding.Ordinal);

            if (binding.Direct && fieldType != null)
            {
                if (fieldType == target)
                    return Expression.Convert(TypedGet(reader, fieldType, ordinal), propertyType);
                if (IsNumeric(fieldType) && IsNumeric(target))
                    return Expression.Convert(Expression.Convert(TypedGet(reader, fieldType, ordinal), target), propertyType);
            }

            Expression raw = Expression.Call(reader, _GetValue, ordinal);
            Expression viaConverter = Expression.Call(converter, _ConvertFromDatabase, raw, Expression.Constant(propertyType, typeof(Type)), Expression.Constant(column, typeof(ColumnMetadata)));
            Expression converterValue = propertyType.IsValueType && Nullable.GetUnderlyingType(propertyType) == null
                ? Expression.Unbox(viaConverter, propertyType)
                : Expression.Convert(viaConverter, propertyType);

            if (inline && fieldType != null && column.Converter == null && !column.IsJson)
            {
                Expression? converted = InlineConversion(reader, target, fieldType, ordinal);
                if (converted != null)
                {
                    // The inlined rule is only valid for the driver type it was built for; a row whose value has another
                    // runtime type (SQLite storage classes vary per row) takes the converter for that value.
                    Expression sameType = Expression.Equal(Expression.Call(reader, _GetFieldType, ordinal), Expression.Constant(fieldType, typeof(Type)));
                    return Expression.Condition(sameType, Expression.Convert(converted, propertyType), converterValue);
                }
            }

            return converterValue;
        }

        // Each case reproduces DataTypeConverter.ConvertFromDatabase for a non-null value of the given driver type.
        [RequiresDynamicCode("Compiles expression trees with generic instantiations chosen at run time; RowMaterializer only calls this when RuntimeFeature.IsDynamicCodeSupported is true and otherwise materializes rows with cached reflection accessors.")]
        private static Expression? InlineConversion(ParameterExpression reader, Type target, Type fieldType, Expression ordinal)
        {
            if (target.IsEnum)
            {
                if (fieldType == typeof(string))
                    return Expression.Call(_EnumParse.MakeGenericMethod(target), Expression.Call(reader, _GetString, ordinal), Expression.Constant(true));
                if (IsIntegral(fieldType))
                    return Expression.Convert(Expression.ConvertChecked(TypedGet(reader, fieldType, ordinal), Enum.GetUnderlyingType(target)), target);
                return null;
            }

            if (target == typeof(bool) && IsNumeric(fieldType))
                return Expression.NotEqual(TypedGet(reader, fieldType, ordinal), Expression.Convert(Expression.Constant(0), fieldType));

            if (fieldType == typeof(string))
            {
                if (target == typeof(Guid)) return Expression.Call(_GuidParse, Expression.Call(reader, _GetString, ordinal));
                if (target == typeof(DateTime)) return Expression.Call(_ParseDateTime, Expression.Call(reader, _GetString, ordinal));
                if (target == typeof(DateTimeOffset)) return Expression.Call(_ParseDateTimeOffset, Expression.Call(reader, _GetString, ordinal));
                return null;
            }

            if (target == typeof(TimeSpan) && fieldType == typeof(long))
                return Expression.Call(_TimeSpanFromTicks, TypedGet(reader, fieldType, ordinal));
            if (target == typeof(DateOnly) && fieldType == typeof(DateTime))
                return Expression.Call(_DateOnlyFromDateTime, TypedGet(reader, fieldType, ordinal));
            if (target == typeof(TimeOnly) && fieldType == typeof(TimeSpan))
                return Expression.Call(_TimeOnlyFromTimeSpan, TypedGet(reader, fieldType, ordinal));
            return null;
        }

        private static bool IsIntegral(Type type)
        {
            return type == typeof(byte) || type == typeof(sbyte) || type == typeof(short) || type == typeof(ushort)
                || type == typeof(int) || type == typeof(uint) || type == typeof(long) || type == typeof(ulong);
        }

        private static bool IsNumeric(Type type)
        {
            return IsIntegral(type) || type == typeof(float) || type == typeof(double) || type == typeof(decimal);
        }
    }
}
