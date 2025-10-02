namespace Durable.SqlServer
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Reflection;
    using System.Text.Json;
    using Durable;

    /// <summary>
    /// SQL Server-specific data type converter that maintains type fidelity for SQL Server parameter binding.
    /// Handles SQL Server-specific types including UNIQUEIDENTIFIER, BIT, DATETIME2, and NVARCHAR.
    /// </summary>
    public class SqlServerDataTypeConverter : IDataTypeConverter
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
        /// Converts a .NET object to its SQL Server database parameter representation.
        /// Preserves DateTime objects as DateTime for proper SQL Server parameter binding.
        /// </summary>
        /// <param name="value">The value to convert.</param>
        /// <param name="targetType">The target database type.</param>
        /// <param name="propertyInfo">Optional property information for attribute-based conversion hints.</param>
        /// <returns>The SQL Server-compatible representation of the value.</returns>
        public object ConvertToDatabase(object value, Type targetType, PropertyInfo? propertyInfo = null)
        {
            if (value == null)
                return DBNull.Value;

            Type valueType = value.GetType();

            // DateTime handling - preserve as DateTime for SQL Server parameter binding
            if (valueType == typeof(DateTime))
            {
                return value; // Keep as DateTime object for datetime2 binding
            }

            // DateTimeOffset handling - preserve as DateTimeOffset for SQL Server parameter binding
            if (valueType == typeof(DateTimeOffset))
            {
                return value; // Keep as DateTimeOffset object for datetimeoffset binding
            }

            // DateOnly handling (.NET 6+) - convert to DateTime for SQL Server
            if (valueType.Name == "DateOnly")
            {
                dynamic dateOnly = value;
                return dateOnly.ToDateTime(TimeOnly.MinValue);
            }

            // TimeOnly handling (.NET 6+) - convert to TimeSpan for SQL Server
            if (valueType.Name == "TimeOnly")
            {
                dynamic timeOnly = value;
                return timeOnly.ToTimeSpan();
            }

            // TimeSpan handling - preserve as TimeSpan for SQL Server time type
            if (valueType == typeof(TimeSpan))
            {
                return value; // Keep as TimeSpan object
            }

            // Guid handling - preserve as Guid for SQL Server uniqueidentifier type
            if (valueType == typeof(Guid))
            {
                return value; // Keep as Guid object
            }

            // Boolean handling - SQL Server uses BIT (0/1)
            if (valueType == typeof(bool))
            {
                return value; // Keep as bool, ADO.NET handles BIT conversion
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

            // Array and Collection handling - serialize to JSON for SQL Server nvarchar(max)
            if (valueType.IsArray || (valueType.IsGenericType &&
                (typeof(IEnumerable).IsAssignableFrom(valueType) && valueType != typeof(string))))
            {
                return JsonSerializer.Serialize(value, JsonOptions);
            }

            // Complex object handling - serialize to JSON for SQL Server nvarchar(max)
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
        /// Converts a SQL Server database value to its .NET type representation.
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

            // DateTime handling - SQL Server returns DateTime objects directly
            if (targetType == typeof(DateTime))
            {
                if (value is DateTime dt)
                    return dt;
                if (value is string dateStr)
                    return DateTime.Parse(dateStr, CultureInfo.InvariantCulture);
                return Convert.ToDateTime(value);
            }

            // DateTimeOffset handling - SQL Server returns DateTimeOffset objects directly
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

            // TimeSpan handling - SQL Server returns TimeSpan objects for time type
            if (targetType == typeof(TimeSpan))
            {
                if (value is TimeSpan ts)
                    return ts;
                if (value is string tsStr)
                    return TimeSpan.Parse(tsStr, CultureInfo.InvariantCulture);
                return TimeSpan.Parse(value.ToString()!, CultureInfo.InvariantCulture);
            }

            // Guid handling - SQL Server returns Guid objects for uniqueidentifier type
            if (targetType == typeof(Guid))
            {
                if (value is Guid guid)
                    return guid;
                if (value is string guidStr)
                    return Guid.Parse(guidStr);
                return new Guid(value.ToString()!);
            }

            // Boolean handling - SQL Server BIT type returns bool or int
            if (targetType == typeof(bool))
            {
                if (value is bool boolVal)
                    return boolVal;
                if (value is int intVal)
                    return intVal != 0;
                if (value is byte byteVal)
                    return byteVal != 0;
                if (value is string strVal)
                    return bool.Parse(strVal);
                return Convert.ToBoolean(value);
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
        /// Gets the appropriate SQL Server database type string for the specified .NET type.
        /// </summary>
        /// <param name="type">The .NET type.</param>
        /// <param name="propertyInfo">Optional property information for attribute-based type mapping hints.</param>
        /// <returns>The SQL Server type string (e.g., "NVARCHAR(MAX)", "INT", "DATETIME2").</returns>
        public string GetDatabaseTypeString(Type type, PropertyInfo? propertyInfo = null)
        {
            // Nullable handling
            type = Nullable.GetUnderlyingType(type) ?? type;

            // Check for PropertyAttribute
            PropertyAttribute? attr = propertyInfo?.GetCustomAttribute<PropertyAttribute>();
            if (attr != null && (attr.PropertyFlags & Flags.String) == Flags.String)
            {
                return "NVARCHAR(MAX)";
            }

            // SQL Server type mappings
            if (type == typeof(bool))
                return "BIT";
            if (type == typeof(byte))
                return "TINYINT";
            if (type == typeof(sbyte))
                return "SMALLINT";
            if (type == typeof(short))
                return "SMALLINT";
            if (type == typeof(ushort))
                return "INT";
            if (type == typeof(int))
                return "INT";
            if (type == typeof(uint))
                return "BIGINT";
            if (type == typeof(long))
                return "BIGINT";
            if (type == typeof(ulong))
                return "DECIMAL(20,0)";
            if (type == typeof(float))
                return "REAL";
            if (type == typeof(double))
                return "FLOAT";
            if (type == typeof(decimal))
                return "DECIMAL(18,4)";
            if (type == typeof(DateTime))
                return "DATETIME2";
            if (type == typeof(DateTimeOffset))
                return "DATETIMEOFFSET";
            if (type.Name == "DateOnly")
                return "DATE";
            if (type.Name == "TimeOnly")
                return "TIME";
            if (type == typeof(TimeSpan))
                return "TIME";
            if (type == typeof(Guid))
                return "UNIQUEIDENTIFIER";
            if (type == typeof(string))
                return "NVARCHAR(MAX)";
            if (type == typeof(byte[]))
                return "VARBINARY(MAX)";
            if (type.IsEnum)
            {
                if (attr != null && (attr.PropertyFlags & Flags.String) != Flags.String)
                    return "INT";
                return "NVARCHAR(MAX)";
            }
            if (type.IsArray || (type.IsGenericType && typeof(IEnumerable).IsAssignableFrom(type)))
                return "NVARCHAR(MAX)";

            // Complex types stored as JSON in NVARCHAR(MAX)
            return "NVARCHAR(MAX)";
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
