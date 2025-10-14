namespace Durable.MySql
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Reflection;
    using System.Text.Json;
    using Durable;

    /// <summary>
    /// MySQL-specific data type converter that handles MySQL's unique type representations.
    /// Provides special handling for TINYINT(1) booleans, JSON columns, unsigned integers, ENUM/SET types, and MySQL datetime types.
    /// </summary>
    public class MySqlDataTypeConverter : IDataTypeConverter
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
        /// Converts a .NET object to its MySQL database parameter representation.
        /// Preserves native types where possible for proper MySQL parameter binding.
        /// </summary>
        /// <param name="value">The value to convert.</param>
        /// <param name="targetType">The target database type.</param>
        /// <param name="propertyInfo">Optional property information for attribute-based conversion hints.</param>
        /// <returns>The MySQL-compatible representation of the value.</returns>
        public object ConvertToDatabase(object value, Type targetType, PropertyInfo? propertyInfo = null)
        {
            if (value == null)
                return DBNull.Value;

            Type valueType = value.GetType();

            // Boolean handling - MySQL uses TINYINT(1) for booleans
            if (valueType == typeof(bool))
            {
                return (bool)value ? (sbyte)1 : (sbyte)0;
            }

            // DateTime handling - preserve as DateTime for MySQL parameter binding
            // MySQL supports both DATETIME and TIMESTAMP types
            if (valueType == typeof(DateTime))
            {
                return value; // Keep as DateTime object
            }

            // DateTimeOffset handling - MySQL doesn't have native DateTimeOffset, convert to DateTime
            if (valueType == typeof(DateTimeOffset))
            {
                DateTimeOffset dto = (DateTimeOffset)value;
                // Store as UTC DateTime for consistency
                return dto.UtcDateTime;
            }

            // DateOnly handling (.NET 6+) - convert to DateTime for MySQL DATE type
            if (valueType.Name == "DateOnly")
            {
                dynamic dateOnly = value;
                // Convert to DateTime at midnight
                return new DateTime(dateOnly.Year, dateOnly.Month, dateOnly.Day, 0, 0, 0, DateTimeKind.Unspecified);
            }

            // TimeOnly handling (.NET 6+) - convert to TimeSpan for MySQL TIME type
            if (valueType.Name == "TimeOnly")
            {
                dynamic timeOnly = value;
                return timeOnly.ToTimeSpan();
            }

            // TimeSpan handling - preserve as TimeSpan for MySQL TIME type
            if (valueType == typeof(TimeSpan))
            {
                return value; // Keep as TimeSpan object
            }

            // Guid handling - MySQL doesn't have native UUID type, store as CHAR(36)
            if (valueType == typeof(Guid))
            {
                return value.ToString()!; // Convert to string representation
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
                // Default to string representation for MySQL ENUM type compatibility
                return value.ToString()!;
            }

            // Array and Collection handling - serialize to JSON for MySQL JSON type
            if (valueType.IsArray || (valueType.IsGenericType &&
                (typeof(IEnumerable).IsAssignableFrom(valueType) && valueType != typeof(string))))
            {
                return JsonSerializer.Serialize(value, JsonOptions);
            }

            // Complex object handling - serialize to JSON for MySQL JSON type
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
        /// Converts a MySQL database value to its .NET type representation.
        /// Handles MySQL-specific types including TINYINT(1) booleans, JSON columns, and unsigned integers.
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

            // Boolean handling - MySQL TINYINT(1) returns as sbyte or byte
            if (targetType == typeof(bool))
            {
                if (value is sbyte sb)
                    return sb != 0;
                if (value is byte b)
                    return b != 0;
                if (value is short s)
                    return s != 0;
                if (value is int i)
                    return i != 0;
                if (value is long l)
                    return l != 0;
                if (value is string boolStr)
                    return boolStr != "0" && !string.Equals(boolStr, "false", StringComparison.OrdinalIgnoreCase);
                return Convert.ToBoolean(value);
            }

            // DateTime handling - MySQL returns DateTime objects directly for DATETIME and TIMESTAMP
            if (targetType == typeof(DateTime))
            {
                if (value is DateTime dt)
                    return dt;
                if (value is string dateStr)
                    return DateTime.Parse(dateStr, CultureInfo.InvariantCulture);
                return Convert.ToDateTime(value);
            }

            // DateTimeOffset handling - MySQL doesn't have native DateTimeOffset, convert from DateTime
            if (targetType == typeof(DateTimeOffset))
            {
                if (value is DateTimeOffset dto)
                    return dto;
                if (value is DateTime dt)
                {
                    // Assume UTC if unspecified
                    if (dt.Kind == DateTimeKind.Unspecified)
                        dt = DateTime.SpecifyKind(dt, DateTimeKind.Utc);
                    return new DateTimeOffset(dt);
                }
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
                    MethodInfo? fromDateTimeMethod = dateOnlyType.GetMethod("FromDateTime", new[] { typeof(DateTime) });
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
                    MethodInfo? fromTimeSpanMethod = timeOnlyType.GetMethod("FromTimeSpan", new[] { typeof(TimeSpan) });
                    return fromTimeSpanMethod?.Invoke(null, new object[] { tsValue });
                }
                if (value is string timeStr)
                {
                    Type timeOnlyType = targetType;
                    MethodInfo? parseMethod = timeOnlyType.GetMethod("Parse", new[] { typeof(string), typeof(IFormatProvider) });
                    return parseMethod?.Invoke(null, new object[] { timeStr, CultureInfo.InvariantCulture });
                }
            }

            // TimeSpan handling - MySQL returns TimeSpan objects for TIME type
            // MySQL TIME can store values from '-838:59:59' to '838:59:59' (extended range)
            if (targetType == typeof(TimeSpan))
            {
                if (value is TimeSpan ts)
                    return ts;
                if (value is string tsStr)
                {
                    // Try to parse MySQL TIME format which can have hours > 24
                    // Format: [-]HH:MM:SS[.fraction] or [-]HHH:MM:SS[.fraction]
                    if (TryParseMySqlTime(tsStr, out TimeSpan result))
                        return result;
                    // Fallback to standard parsing
                    return TimeSpan.Parse(tsStr, CultureInfo.InvariantCulture);
                }
                // Fallback to standard parsing
                return TimeSpan.Parse(value.ToString()!, CultureInfo.InvariantCulture);
            }

            // Guid handling - MySQL stores as CHAR(36) string
            if (targetType == typeof(Guid))
            {
                if (value is Guid guid)
                    return guid;
                if (value is string guidStr)
                    return Guid.Parse(guidStr);
                return new Guid(value.ToString()!);
            }

            // Enum handling - MySQL ENUM type returns as string, numeric storage returns as integer
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

            // Unsigned integer handling - MySQL BIGINT UNSIGNED can exceed signed long
            if (targetType == typeof(ulong))
            {
                if (value is ulong ul)
                    return ul;
                if (value is string ulStr)
                    return ulong.Parse(ulStr, CultureInfo.InvariantCulture);
                // Handle cases where MySQL returns as signed but should be unsigned
                if (value is long l && l >= 0)
                    return (ulong)l;
                return Convert.ToUInt64(value);
            }

            // Other unsigned types
            if (targetType == typeof(uint))
            {
                if (value is uint ui)
                    return ui;
                if (value is int i && i >= 0)
                    return (uint)i;
                return Convert.ToUInt32(value);
            }

            if (targetType == typeof(ushort))
            {
                if (value is ushort us)
                    return us;
                if (value is short s && s >= 0)
                    return (ushort)s;
                return Convert.ToUInt16(value);
            }

            // Array and Collection handling - deserialize from MySQL JSON column
            if (targetType.IsArray || (targetType.IsGenericType &&
                (typeof(IEnumerable).IsAssignableFrom(targetType) && targetType != typeof(string))))
            {
                if (value is string jsonStr && !string.IsNullOrWhiteSpace(jsonStr))
                {
                    return JsonSerializer.Deserialize(jsonStr, targetType, JsonOptions);
                }
                // Return null for empty/null JSON values for nullable types
                return null;
            }

            // Complex object handling - deserialize from MySQL JSON column
            if (!IsSimpleType(targetType) && value is string json)
            {
                if (!string.IsNullOrWhiteSpace(json))
                {
                    return JsonSerializer.Deserialize(json, targetType, JsonOptions);
                }
                // Return null for empty/null JSON values
                return null;
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
        /// Gets the appropriate MySQL database type string for the specified .NET type.
        /// </summary>
        /// <param name="type">The .NET type.</param>
        /// <param name="propertyInfo">Optional property information for attribute-based type mapping hints.</param>
        /// <returns>The MySQL type string (e.g., "VARCHAR(255)", "INT", "DATETIME", "JSON").</returns>
        public string GetDatabaseTypeString(Type type, PropertyInfo? propertyInfo = null)
        {
            // Nullable handling
            type = Nullable.GetUnderlyingType(type) ?? type;

            // Check for PropertyAttribute
            PropertyAttribute? attr = propertyInfo?.GetCustomAttribute<PropertyAttribute>();
            if (attr != null && (attr.PropertyFlags & Flags.String) == Flags.String)
            {
                return "TEXT";
            }

            // MySQL type mappings
            if (type == typeof(bool))
                return "TINYINT(1)";
            if (type == typeof(byte))
                return "TINYINT UNSIGNED";
            if (type == typeof(sbyte))
                return "TINYINT";
            if (type == typeof(short))
                return "SMALLINT";
            if (type == typeof(ushort))
                return "SMALLINT UNSIGNED";
            if (type == typeof(int))
                return "INT";
            if (type == typeof(uint))
                return "INT UNSIGNED";
            if (type == typeof(long))
                return "BIGINT";
            if (type == typeof(ulong))
                return "BIGINT UNSIGNED";
            if (type == typeof(float))
                return "FLOAT";
            if (type == typeof(double))
                return "DOUBLE";
            if (type == typeof(decimal))
                return "DECIMAL(65,30)";
            if (type == typeof(DateTime))
                return "DATETIME(6)"; // 6 digits for microsecond precision
            if (type == typeof(DateTimeOffset))
                return "DATETIME(6)"; // MySQL doesn't have native DateTimeOffset, use DATETIME with UTC
            if (type.Name == "DateOnly")
                return "DATE";
            if (type.Name == "TimeOnly")
                return "TIME(6)"; // 6 digits for microsecond precision
            if (type == typeof(TimeSpan))
                return "TIME(6)";
            if (type == typeof(Guid))
                return "CHAR(36)"; // Standard UUID string format
            if (type == typeof(string))
                return "TEXT";
            if (type.IsEnum)
            {
                if (attr != null && (attr.PropertyFlags & Flags.String) != Flags.String)
                    return "INT";
                // Use VARCHAR instead of ENUM to avoid MySQL ENUM limitations
                return "VARCHAR(255)";
            }
            if (type.IsArray || (type.IsGenericType && typeof(IEnumerable).IsAssignableFrom(type)))
                return "JSON"; // MySQL 5.7+ JSON column type

            // Complex types stored as JSON
            return "JSON";
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

        private bool TryParseMySqlTime(string value, out TimeSpan result)
        {
            result = TimeSpan.Zero;

            if (string.IsNullOrWhiteSpace(value))
                return false;

            try
            {
                // MySQL TIME format: [-]HHH:MM:SS[.fraction]
                // Can have negative values and hours > 24
                bool isNegative = value.StartsWith("-");
                string timeValue = isNegative ? value.Substring(1) : value;

                string[] parts = timeValue.Split(':');
                if (parts.Length != 3)
                    return false;

                if (!int.TryParse(parts[0], out int hours))
                    return false;

                if (!int.TryParse(parts[1], out int minutes))
                    return false;

                // Handle seconds with optional fractional part
                string[] secondsParts = parts[2].Split('.');
                if (!int.TryParse(secondsParts[0], out int seconds))
                    return false;

                int microseconds = 0;
                if (secondsParts.Length > 1)
                {
                    // MySQL stores microseconds (6 digits), .NET uses ticks (10000 ticks = 1 ms)
                    // Pad or truncate to 6 digits then convert to ticks
                    string fractionStr = secondsParts[1].PadRight(6, '0').Substring(0, 6);
                    if (int.TryParse(fractionStr, out int fraction))
                    {
                        // Convert microseconds to ticks (1 microsecond = 10 ticks)
                        microseconds = fraction;
                    }
                }

                // Build TimeSpan from components
                // TimeSpan constructor: days, hours, minutes, seconds, milliseconds
                // We need to convert total hours to days + hours
                int days = hours / 24;
                int remainingHours = hours % 24;
                int milliseconds = microseconds / 1000;
                int remainingMicroseconds = microseconds % 1000;

                result = new TimeSpan(days, remainingHours, minutes, seconds, milliseconds);

                // Add remaining microseconds as ticks (1 microsecond = 10 ticks)
                result = result.Add(TimeSpan.FromTicks(remainingMicroseconds * 10));

                if (isNegative)
                    result = result.Negate();

                return true;
            }
            catch
            {
                return false;
            }
        }

        #endregion
    }
}
