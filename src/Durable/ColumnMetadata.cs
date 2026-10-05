namespace Durable
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;

    /// <summary>
    /// Immutable description of a mapped column, built once per entity type and cached.
    /// Thread safety: instances are immutable after construction and safe to share across threads.
    /// </summary>
    public sealed class ColumnMetadata
    {
        #region Public-Members

        /// <summary>
        /// Gets the column name. Never null.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Gets the mapped property. Never null.
        /// </summary>
        public PropertyInfo Property { get; }

        /// <summary>
        /// Gets the declared property type (may be a <see cref="Nullable{T}"/>).
        /// </summary>
        public Type PropertyType { get; }

        /// <summary>
        /// Gets the property type with any <see cref="Nullable{T}"/> wrapper removed.
        /// </summary>
        public Type ClrType { get; }

        /// <summary>
        /// Gets the column flags.
        /// </summary>
        public Flags Flags { get; }

        /// <summary>
        /// Gets the maximum length for string columns; zero for no limit.
        /// </summary>
        public int MaxLength { get; }

        /// <summary>
        /// Gets whether the column is part of the primary key.
        /// </summary>
        public bool IsPrimaryKey { get; }

        /// <summary>
        /// Gets the position of the column within the primary key; zero-based. -1 when not a key column.
        /// </summary>
        public int KeyOrdinal { get; internal set; } = -1;

        /// <summary>
        /// Gets whether the database generates the value on insert.
        /// </summary>
        public bool IsAutoIncrement { get; }

        /// <summary>
        /// Gets whether the column accepts nulls: true for nullable value types and for reference types not annotated as non-nullable.
        /// </summary>
        public bool IsNullable { get; }

        /// <summary>
        /// Gets whether the property is an enum (or nullable enum).
        /// </summary>
        public bool IsEnum { get; }

        /// <summary>
        /// Gets whether an enum property is stored by name. False when <see cref="Durable.Flags.Integer"/> is set.
        /// </summary>
        public bool EnumAsString { get; }

        /// <summary>
        /// Gets whether the value is stored as JSON (explicit <see cref="Durable.Flags.Json"/>, collections, or complex objects).
        /// </summary>
        public bool IsJson { get; }

        /// <summary>
        /// Gets whether this is the optimistic-concurrency version column.
        /// </summary>
        public bool IsVersion { get; }

        /// <summary>
        /// Gets whether this is the soft-delete marker column.
        /// </summary>
        public bool IsSoftDelete { get; }

        /// <summary>
        /// Gets the custom value converter for this column, or null when built-in conversion applies.
        /// </summary>
        public IValueConverter? Converter { get; }

        /// <summary>
        /// Gets the foreign key declaration on this column, or null.
        /// </summary>
        public ForeignKeyAttribute? ForeignKey { get; }

        /// <summary>
        /// Gets the default value provider for this column, or null.
        /// </summary>
        public DefaultValueProviderInfo? DefaultValue { get; }

        /// <summary>
        /// Gets the single-column index declarations on this property. Never null; may be empty.
        /// </summary>
        public IReadOnlyList<IndexAttribute> Indexes { get; }

        #endregion

        #region Private-Members

        private readonly Func<object, object?> _Getter;
        private readonly Action<object, object?> _Setter;

        #endregion

        #region Constructors-and-Factories

        internal ColumnMetadata(
            string name,
            PropertyInfo property,
            Flags flags,
            int maxLength,
            bool isPrimaryKey,
            bool isAutoIncrement,
            bool isNullable,
            bool isJson,
            bool isVersion,
            bool isSoftDelete,
            IValueConverter? converter,
            ForeignKeyAttribute? foreignKey,
            DefaultValueProviderInfo? defaultValue,
            IReadOnlyList<IndexAttribute> indexes)
        {
            Name = name;
            Property = property;
            PropertyType = property.PropertyType;
            ClrType = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;
            Flags = flags;
            MaxLength = maxLength;
            IsPrimaryKey = isPrimaryKey;
            IsAutoIncrement = isAutoIncrement;
            IsNullable = isNullable;
            IsEnum = ClrType.IsEnum;
            EnumAsString = IsEnum && (flags & Flags.Integer) != Flags.Integer;
            IsJson = isJson;
            IsVersion = isVersion;
            IsSoftDelete = isSoftDelete;
            Converter = converter;
            ForeignKey = foreignKey;
            DefaultValue = defaultValue;
            Indexes = indexes;
            _Getter = MemberAccessorFactory.CreateGetter(property);
            _Setter = MemberAccessorFactory.CreateSetter(property);
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Reads the property value from an entity using a compiled accessor.
        /// </summary>
        /// <param name="entity">Entity instance. Must not be null.</param>
        /// <returns>The property value; may be null.</returns>
        public object? GetValue(object entity)
        {
            return _Getter(entity);
        }

        /// <summary>
        /// Writes the property value on an entity using a compiled accessor.
        /// A null value assigned to a non-nullable value type sets the type's default.
        /// </summary>
        /// <param name="entity">Entity instance. Must not be null.</param>
        /// <param name="value">Value to assign; may be null.</param>
        public void SetValue(object entity, object? value)
        {
            _Setter(entity, value);
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return Name + " (" + Property.Name + ")";
        }

        #endregion
    }
}
