namespace Durable
{
    using System;
    using System.Buffers.Binary;
    using System.Reflection;

    /// <summary>
    /// The resolved version column of an entity (<see cref="EntityMetadata.VersionInfo"/>): its column, property,
    /// effective <see cref="VersionColumnType"/>, and how new values are produced.
    /// Thread safety: immutable; safe to share.
    /// </summary>
    public class VersionColumnInfo
    {
        #region Public-Members

        /// <summary>
        /// Gets the database column name. Never null.
        /// </summary>
        public string ColumnName { get; }

        /// <summary>
        /// Gets the mapped property. Never null.
        /// </summary>
        public PropertyInfo Property { get; }

        /// <summary>
        /// Gets the effective version type (declared, or inferred from the property type).
        /// </summary>
        public VersionColumnType Type { get; }

        /// <summary>
        /// Gets the property type. Never null.
        /// </summary>
        public Type PropertyType { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the version column description, validating that the property type suits the version type.
        /// </summary>
        /// <param name="columnName">Column name. Must not be null.</param>
        /// <param name="property">Property. Must not be null.</param>
        /// <param name="declaredType">Declared version type, or null to infer it from the property type.</param>
        /// <exception cref="ArgumentNullException">Thrown when columnName or property is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the type cannot be inferred or does not suit the property type.</exception>
        public VersionColumnInfo(string columnName, PropertyInfo property, VersionColumnType? declaredType)
        {
            ColumnName = columnName ?? throw new ArgumentNullException(nameof(columnName));
            Property = property ?? throw new ArgumentNullException(nameof(property));
            PropertyType = property.PropertyType;
            Type = declaredType ?? Infer(property);
            if (!Suits(Type, PropertyType))
            {
                throw new InvalidOperationException(
                    "Version column " + property.DeclaringType?.Name + "." + property.Name + " of type " + PropertyType.Name
                    + " cannot use VersionColumnType." + Type + " (Integer: int/long/short/byte; Timestamp: DateTime; Guid: Guid; BinaryCounter: byte[]).");
            }
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Gets the version value from the specified entity.
        /// </summary>
        /// <param name="entity">The entity; null returns null.</param>
        /// <returns>The version value; may be null.</returns>
        public object? GetValue(object entity)
        {
            if (entity == null) return null;
            return Property.GetValue(entity);
        }

        /// <summary>
        /// Sets the version value on the specified entity.
        /// </summary>
        /// <param name="entity">The entity; null does nothing.</param>
        /// <param name="value">The version value.</param>
        public void SetValue(object entity, object value)
        {
            if (entity == null) return;
            Property.SetValue(entity, value);
        }

        /// <summary>
        /// Returns the version value that replaces <paramref name="currentVersion"/> on an entity update.
        /// </summary>
        /// <param name="currentVersion">The current value; null yields <see cref="GetDefaultVersion"/>.</param>
        /// <returns>The next value. Never null.</returns>
        public object IncrementVersion(object? currentVersion)
        {
            if (currentVersion == null) return GetDefaultVersion();

            switch (Type)
            {
                case VersionColumnType.Integer:
                    if (PropertyType == typeof(int)) return (int)currentVersion + 1;
                    if (PropertyType == typeof(long)) return (long)currentVersion + 1;
                    if (PropertyType == typeof(short)) return (short)((short)currentVersion + 1);
                    return (byte)((byte)currentVersion + 1);
                case VersionColumnType.Timestamp:
                    return DateTime.UtcNow;
                case VersionColumnType.Guid:
                    return Guid.NewGuid();
                case VersionColumnType.BinaryCounter:
                default:
                    byte[] currentBytes = (byte[])currentVersion;
                    byte[] newBytes = new byte[currentBytes.Length == 0 ? 8 : currentBytes.Length];
                    Array.Copy(currentBytes, newBytes, currentBytes.Length);
                    for (int i = newBytes.Length - 1; i >= 0; i--)
                    {
                        if (newBytes[i] < 255)
                        {
                            newBytes[i]++;
                            break;
                        }

                        newBytes[i] = 0;
                    }

                    return newBytes;
            }
        }

        /// <summary>
        /// Returns the version value of a newly created row: 1 for integers, the current UTC time, a new GUID, or the
        /// 8-byte counter value 1.
        /// </summary>
        /// <returns>The initial value. Never null.</returns>
        public object GetDefaultVersion()
        {
            switch (Type)
            {
                case VersionColumnType.Integer:
                    if (PropertyType == typeof(int)) return 1;
                    if (PropertyType == typeof(long)) return 1L;
                    if (PropertyType == typeof(short)) return (short)1;
                    return (byte)1;
                case VersionColumnType.Timestamp:
                    return DateTime.UtcNow;
                case VersionColumnType.Guid:
                    return Guid.NewGuid();
                case VersionColumnType.BinaryCounter:
                default:
                    return new byte[] { 0, 0, 0, 0, 0, 0, 0, 1 };
            }
        }

        /// <summary>
        /// Returns the value a set-based update (UpdateField, BatchUpdate) writes to every affected row, for types that
        /// are not incremented in the database: the current UTC time, a new GUID, or an 8-byte counter value derived from
        /// the current UTC ticks (larger than any per-row increment of a counter that started at 1).
        /// </summary>
        /// <returns>The value. Never null.</returns>
        /// <exception cref="InvalidOperationException">Thrown for <see cref="VersionColumnType.Integer"/>, which set-based updates increment in the database.</exception>
        public object CreateSetBasedVersion()
        {
            switch (Type)
            {
                case VersionColumnType.Timestamp:
                    return DateTime.UtcNow;
                case VersionColumnType.Guid:
                    return Guid.NewGuid();
                case VersionColumnType.BinaryCounter:
                    byte[] bytes = new byte[8];
                    BinaryPrimitives.WriteInt64BigEndian(bytes, DateTime.UtcNow.Ticks);
                    return bytes;
                default:
                    throw new InvalidOperationException("Integer version columns are incremented in the database by set-based updates.");
            }
        }

        #endregion

        #region Private-Methods

        private static VersionColumnType Infer(PropertyInfo property)
        {
            Type type = property.PropertyType;
            if (type == typeof(int) || type == typeof(long) || type == typeof(short) || type == typeof(byte)) return VersionColumnType.Integer;
            if (type == typeof(DateTime)) return VersionColumnType.Timestamp;
            if (type == typeof(Guid)) return VersionColumnType.Guid;
            if (type == typeof(byte[])) return VersionColumnType.BinaryCounter;
            throw new InvalidOperationException(
                "Cannot infer the version type of " + property.DeclaringType?.Name + "." + property.Name + " (" + type.Name
                + "); use int/long/short/byte, DateTime, Guid or byte[], or pass a VersionColumnType.");
        }

        private static bool Suits(VersionColumnType versionType, Type propertyType)
        {
            switch (versionType)
            {
                case VersionColumnType.Integer:
                    return propertyType == typeof(int) || propertyType == typeof(long) || propertyType == typeof(short) || propertyType == typeof(byte);
                case VersionColumnType.Timestamp:
                    return propertyType == typeof(DateTime);
                case VersionColumnType.Guid:
                    return propertyType == typeof(Guid);
                case VersionColumnType.BinaryCounter:
                    return propertyType == typeof(byte[]);
                default:
                    return false;
            }
        }

        #endregion
    }
}
