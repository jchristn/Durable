namespace Durable.Postgres
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Reflection;
    using System.Text.Json;
    using Durable;

    /// <summary>
    /// PostgreSQL-specific data type converter that maintains type fidelity for PostgreSQL parameter binding.
    /// Unlike the generic DataTypeConverter, this preserves DateTime objects as DateTime for proper PostgreSQL parameter binding.
    /// </summary>
    public class PostgresDataTypeConverter : IDataTypeConverter
    {

        #region Private-Members

        private static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };

        #endregion

        #region Constructors-and-Factories

        #endregion

        #region Public-Methods

        /// <summary>
        /// Converts a .NET object to its PostgreSQL database parameter representation.
        /// Preserves DateTime objects as DateTime for proper PostgreSQL parameter binding.
        /// </summary>
        /// <param name="value">The value to convert.</param>
        /// <param name="targetType">The target database type.</param>
        /// <param name="propertyInfo">Optional property information for attribute-based conversion hints.</param>
        /// <returns>The PostgreSQL-compatible representation of the value.</returns>
        public object ConvertToDatabase(object value, Type targetType, PropertyInfo? propertyInfo = null)
        {
            if (value == null)
                return DBNull.Value;

            Type valueType = value.GetType();

            // DateTime handling - convert to Unspecified to avoid timezone conversion with TIMESTAMP columns
            if (valueType == typeof(DateTime))
            {
                DateTime dt = (DateTime)value;
                // Convert to Unspecified to prevent Npgsql from applying timezone conversion
                // when storing to TIMESTAMP columns (TIMESTAMPTZ handles timezones properly)
                if (dt.Kind != DateTimeKind.Unspecified)
                {
                    return DateTime.SpecifyKind(dt, DateTimeKind.Unspecified);
                }
                return value;
            }

            // DateTimeOffset handling - preserve as DateTimeOffset for PostgreSQL parameter binding
            if (valueType == typeof(DateTimeOffset))
            {
                return value; // Keep as DateTimeOffset object
            }

            // DateOnly handling (.NET 6+) - convert to DateTime for PostgreSQL
            if (valueType.Name == "DateOnly")
            {
                dynamic dateOnly = value;
                return dateOnly.ToDateTime(TimeOnly.MinValue);
            }

            // TimeOnly handling (.NET 6+) - convert to TimeSpan for PostgreSQL
            if (valueType.Name == "TimeOnly")
            {
                dynamic timeOnly = value;
                return timeOnly.ToTimeSpan();
            }

            // TimeSpan handling - preserve as TimeSpan for PostgreSQL interval type
            if (valueType == typeof(TimeSpan))
            {
                return value; // Keep as TimeSpan object
            }

            // Guid handling - check if property is marked as String, otherwise preserve as Guid for PostgreSQL uuid type
            if (valueType == typeof(Guid))
            {
                // Check if property has Flags.String attribute
                if (propertyInfo != null)
                {
                    PropertyAttribute? propAttr = propertyInfo.GetCustomAttribute<PropertyAttribute>();
                    if (propAttr != null && propAttr.PropertyFlags.HasFlag(Flags.String))
                    {
                        // Store as string if property is marked with Flags.String
                        return ((Guid)value).ToString();
                    }
                }
                return value; // Keep as Guid object for UUID columns
            }

            // Enum handling
            if (valueType.IsEnum)
            {
                // Check for PropertyAttribute flags to determine storage preference
                PropertyAttribute? attr = propertyInfo?.GetCustomAttribute<PropertyAttribute>();
                if (attr != null && (attr.PropertyFlags & Flags.String) != Flags.String)
                {
                    // If String flag is NOT set, store as integer
                    return Convert.ToInt32(value);
                }
                // Default to string representation for readability
                return value.ToString()!;
            }

            // Array and Collection handling - serialize to JSON for PostgreSQL jsonb type
            if (valueType.IsArray || (valueType.IsGenericType &&
                (typeof(IEnumerable).IsAssignableFrom(valueType) && valueType != typeof(string))))
            {
                return JsonSerializer.Serialize(value, JsonOptions);
            }

            // Complex object handling - serialize to JSON for PostgreSQL jsonb type
            if (!IsSimpleType(valueType))
            {
                return JsonSerializer.Serialize(value, JsonOptions);
            }

            // Nullable handling
            if (Nullable.GetUnderlyingType(valueType) != null)
            {
                return ConvertToDatabase(value, Nullable.GetUnderlyingType(valueType)!, propertyInfo);
            }

            // Default: return the value as-is for simple types
            return value;
        }

        /// <summary>
        /// Converts a PostgreSQL database value to its .NET type representation.
        /// </summary>
        /// <param name="value">The database value to convert.</param>
        /// <param name="targetType">The target .NET type.</param>
        /// <param name="propertyInfo">Optional property information for attribute-based conversion hints.</param>
        /// <returns>The .NET object representation of the database value.</returns>
        public object? ConvertFromDatabase(object? value, Type targetType, PropertyInfo? propertyInfo = null)
        {
            if (value == null || value == DBNull.Value)
            {
                if (targetType.IsValueType && Nullable.GetUnderlyingType(targetType) == null)
                {
                    return Activator.CreateInstance(targetType)!;
                }
                return null;
            }

            // Handle nullable types
            Type? underlyingType = Nullable.GetUnderlyingType(targetType);
            if (underlyingType != null)
            {
                targetType = underlyingType;
            }

            // DateTime handling - PostgreSQL returns DateTime objects directly
            if (targetType == typeof(DateTime))
            {
                if (value is DateTime dt)
                    return dt;
                if (value is string dateStr)
                    return DateTime.Parse(dateStr, CultureInfo.InvariantCulture);
                return Convert.ToDateTime(value);
            }

            // DateTimeOffset handling - PostgreSQL returns DateTimeOffset objects directly
            if (targetType == typeof(DateTimeOffset))
            {
                if (value is DateTimeOffset dto)
                    return dto;
                if (value is DateTime dt)
                    return new DateTimeOffset(dt);
                if (value is string dtoStr)
                    return DateTimeOffset.Parse(dtoStr, CultureInfo.InvariantCulture);
                return DateTimeOffset.Parse(value.ToString()!, CultureInfo.InvariantCulture);
            }

            // DateOnly handling (.NET 6+)
            if (targetType.Name == "DateOnly")
            {
                if (value is DateTime dtValue)
                {
                    Type dateOnlyType = targetType;
                    MethodInfo? fromDateTimeMethod = dateOnlyType.GetMethod("FromDateTime");
                    return fromDateTimeMethod?.Invoke(null, new object[] { dtValue });
                }
                if (value is string dateStr)
                {
                    Type dateOnlyType = targetType;
                    MethodInfo? parseMethod = dateOnlyType.GetMethod("Parse", new[] { typeof(string), typeof(IFormatProvider) });
                    return parseMethod?.Invoke(null, new object[] { dateStr, CultureInfo.InvariantCulture });
                }
            }

            // TimeOnly handling (.NET 6+)
            if (targetType.Name == "TimeOnly")
            {
                if (value is TimeSpan tsValue)
                {
                    Type timeOnlyType = targetType;
                    MethodInfo? fromTimeSpanMethod = timeOnlyType.GetMethod("FromTimeSpan");
                    return fromTimeSpanMethod?.Invoke(null, new object[] { tsValue });
                }
                if (value is string timeStr)
                {
                    Type timeOnlyType = targetType;
                    MethodInfo? parseMethod = timeOnlyType.GetMethod("Parse", new[] { typeof(string), typeof(IFormatProvider) });
                    return parseMethod?.Invoke(null, new object[] { timeStr, CultureInfo.InvariantCulture });
                }
            }

            // TimeSpan handling - PostgreSQL returns TimeSpan objects for interval type
            if (targetType == typeof(TimeSpan))
            {
                if (value is TimeSpan ts)
                    return ts;
                if (value is string tsStr)
                    return TimeSpan.Parse(tsStr, CultureInfo.InvariantCulture);
                return TimeSpan.Parse(value.ToString()!, CultureInfo.InvariantCulture);
            }

            // Guid handling - PostgreSQL returns Guid objects for uuid type
            if (targetType == typeof(Guid))
            {
                if (value is Guid guid)
                    return guid;
                if (value is string guidStr)
                    return Guid.Parse(guidStr);
                return new Guid(value.ToString()!);
            }

            // Enum handling
            if (targetType.IsEnum)
            {
                if (value is string enumStr)
                {
                    return Enum.Parse(targetType, enumStr);
                }
                if (IsNumericType(value.GetType()))
                {
                    return Enum.ToObject(targetType, value);
                }
            }

            // Boolean handling - PostgreSQL returns native bool, but handle other formats for flexibility
            if (targetType == typeof(bool))
            {
                if (value is bool boolVal)
                    return boolVal;
                if (value is long l)
                    return l != 0;
                if (value is int i)
                    return i != 0;
                if (value is short s)
                    return s != 0;
                if (value is byte b)
                    return b != 0;
                if (value is sbyte sb)
                    return sb != 0;
                if (value is string boolStr)
                    return boolStr != "0" && !string.Equals(boolStr, "false", StringComparison.OrdinalIgnoreCase);
                return Convert.ToBoolean(value);
            }

            // Array and Collection handling - deserialize from JSON
            if (targetType.IsArray || (targetType.IsGenericType &&
                (typeof(IEnumerable).IsAssignableFrom(targetType) && targetType != typeof(string))))
            {
                if (value is string jsonStr)
                {
                    return JsonSerializer.Deserialize(jsonStr, targetType, JsonOptions);
                }
            }

            // Complex object handling - deserialize from JSON
            if (!IsSimpleType(targetType) && value is string json)
            {
                return JsonSerializer.Deserialize(json, targetType, JsonOptions);
            }

            // Default type conversion
            return Convert.ChangeType(value, targetType);
        }

        /// <summary>
        /// Determines whether the converter can handle the specified type.
        /// </summary>
        /// <param name="type">The type to check for conversion support.</param>
        /// <returns>True if the type can be converted; otherwise, false.</returns>
        public bool CanConvert(Type type)
        {
            // We can convert most types
            return true;
        }

        /// <summary>
        /// Gets the appropriate PostgreSQL database type string for the specified .NET type.
        /// </summary>
        /// <param name="type">The .NET type.</param>
        /// <param name="propertyInfo">Optional property information for attribute-based type mapping hints.</param>
        /// <returns>The PostgreSQL type string (e.g., "TEXT", "INTEGER", "TIMESTAMP").</returns>
        public string GetDatabaseTypeString(Type type, PropertyInfo? propertyInfo = null)
        {
            // Nullable handling
            type = Nullable.GetUnderlyingType(type) ?? type;

            // Check for PropertyAttribute
            PropertyAttribute? attr = propertyInfo?.GetCustomAttribute<PropertyAttribute>();

            // PostgreSQL type mappings
            if (type == typeof(bool))
                return "BOOLEAN";
            if (type == typeof(byte) || type == typeof(sbyte))
                return "SMALLINT";
            if (type == typeof(short) || type == typeof(ushort))
                return "SMALLINT";
            if (type == typeof(int) || type == typeof(uint))
                return "INTEGER";
            if (type == typeof(long) || type == typeof(ulong))
                return "BIGINT";
            if (type == typeof(float))
                return "REAL";
            if (type == typeof(double))
                return "DOUBLE PRECISION";
            if (type == typeof(decimal))
                return "NUMERIC";
            if (type == typeof(DateTime))
                return "TIMESTAMP";
            if (type == typeof(DateTimeOffset))
                return "TIMESTAMPTZ";
            if (type.Name == "DateOnly")
                return "DATE";
            if (type.Name == "TimeOnly")
                return "TIME";
            if (type == typeof(TimeSpan))
                return "INTERVAL";
            if (type == typeof(Guid))
                return "UUID";
            if (type == typeof(string))
            {
                // Use VARCHAR if MaxLength is specified, otherwise TEXT
                if (attr != null && attr.MaxLength > 0)
                {
                    return $"VARCHAR({attr.MaxLength})";
                }
                return "TEXT";
            }
            if (type.IsEnum)
            {
                if (attr != null && (attr.PropertyFlags & Flags.String) != Flags.String)
                    return "INTEGER";
                return "TEXT";
            }
            if (type.IsArray || (type.IsGenericType && typeof(IEnumerable).IsAssignableFrom(type)))
                return "JSONB";

            // Complex types stored as JSON
            return "JSONB";
        }

        #endregion

        #region Private-Methods

        private bool IsSimpleType(Type type)
        {
            return type.IsPrimitive
                || type == typeof(string)
                || type == typeof(decimal)
                || type == typeof(DateTime)
                || type == typeof(DateTimeOffset)
                || type == typeof(TimeSpan)
                || type == typeof(Guid)
                || type.IsEnum
                || (Nullable.GetUnderlyingType(type)?.IsPrimitive ?? false);
        }

        private bool IsNumericType(Type type)
        {
            return type == typeof(byte) || type == typeof(sbyte)
                || type == typeof(short) || type == typeof(ushort)
                || type == typeof(int) || type == typeof(uint)
                || type == typeof(long) || type == typeof(ulong)
                || type == typeof(float) || type == typeof(double)
                || type == typeof(decimal);
        }

        #endregion
    }
}