namespace Durable
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Reflection;
    using System.Text.Json;

    public class DataTypeConverter : IDataTypeConverter
    {
        #region Public-Members

        #endregion

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

        public object ConvertToDatabase(object value, Type targetType, PropertyInfo propertyInfo = null)
        {
            if (value == null)
                return DBNull.Value;

            Type valueType = value.GetType();

            // DateTime handling
            if (valueType == typeof(DateTime))
            {
                DateTime dt = (DateTime)value;
                // Store as ISO 8601 string for consistency
                return dt.ToString("yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
            }

            // DateTimeOffset handling
            if (valueType == typeof(DateTimeOffset))
            {
                DateTimeOffset dto = (DateTimeOffset)value;
                // Store with timezone information
                return dto.ToString("yyyy-MM-dd HH:mm:ss.fffffffzzz", CultureInfo.InvariantCulture);
            }

            // DateOnly handling (.NET 6+)
            if (valueType.Name == "DateOnly")
            {
                dynamic dateOnly = value;
                return dateOnly.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
            }

            // TimeOnly handling (.NET 6+)
            if (valueType.Name == "TimeOnly")
            {
                dynamic timeOnly = value;
                return timeOnly.ToString("HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
            }

            // TimeSpan handling
            if (valueType == typeof(TimeSpan))
            {
                TimeSpan ts = (TimeSpan)value;
                return ts.ToString("c", CultureInfo.InvariantCulture);
            }

            // Guid handling
            if (valueType == typeof(Guid))
            {
                return value.ToString();
            }

            // Enum handling
            if (valueType.IsEnum)
            {
                // Check for PropertyAttribute flags to determine storage preference
                PropertyAttribute attr = propertyInfo?.GetCustomAttribute<PropertyAttribute>();
                if (attr != null && (attr.PropertyFlags & Flags.String) != Flags.String)
                {
                    // If String flag is NOT set, store as integer
                    return Convert.ToInt32(value);
                }
                // Default to string representation for readability
                return value.ToString();
            }

            // Array and Collection handling - serialize to JSON
            if (valueType.IsArray || (valueType.IsGenericType && 
                (typeof(IEnumerable).IsAssignableFrom(valueType) && valueType != typeof(string))))
            {
                return JsonSerializer.Serialize(value, JsonOptions);
            }

            // Complex object handling - serialize to JSON
            if (!IsSimpleType(valueType))
            {
                return JsonSerializer.Serialize(value, JsonOptions);
            }

            // Nullable handling
            if (Nullable.GetUnderlyingType(valueType) != null)
            {
                return ConvertToDatabase(value, Nullable.GetUnderlyingType(valueType), propertyInfo);
            }

            // Default: return the value as-is for simple types
            return value;
        }

        public object ConvertFromDatabase(object value, Type targetType, PropertyInfo propertyInfo = null)
        {
            if (value == null || value == DBNull.Value)
            {
                if (targetType.IsValueType && Nullable.GetUnderlyingType(targetType) == null)
                {
                    return Activator.CreateInstance(targetType);
                }
                return null;
            }

            // Handle nullable types
            Type underlyingType = Nullable.GetUnderlyingType(targetType);
            if (underlyingType != null)
            {
                targetType = underlyingType;
            }

            // DateTime handling
            if (targetType == typeof(DateTime))
            {
                if (value is string dateStr)
                {
                    if (DateTime.TryParseExact(dateStr, "yyyy-MM-dd HH:mm:ss.fffffff", 
                        CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime result))
                        return result;
                    // Fallback to general parsing
                    return DateTime.Parse(dateStr, CultureInfo.InvariantCulture);
                }
                return Convert.ToDateTime(value);
            }

            // DateTimeOffset handling
            if (targetType == typeof(DateTimeOffset))
            {
                if (value is string dtoStr)
                {
                    if (DateTimeOffset.TryParseExact(dtoStr, "yyyy-MM-dd HH:mm:ss.fffffffzzz",
                        CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTimeOffset result))
                        return result;
                    // Fallback to general parsing
                    return DateTimeOffset.Parse(dtoStr, CultureInfo.InvariantCulture);
                }
                if (value is DateTime dt)
                {
                    return new DateTimeOffset(dt);
                }
                return DateTimeOffset.Parse(value.ToString(), CultureInfo.InvariantCulture);
            }

            // DateOnly handling (.NET 6+)
            if (targetType.Name == "DateOnly")
            {
                if (value is string dateStr)
                {
                    Type dateOnlyType = targetType;
                    MethodInfo parseMethod = dateOnlyType.GetMethod("Parse", new[] { typeof(string), typeof(IFormatProvider) });
                    return parseMethod.Invoke(null, new object[] { dateStr, CultureInfo.InvariantCulture });
                }
            }

            // TimeOnly handling (.NET 6+)
            if (targetType.Name == "TimeOnly")
            {
                if (value is string timeStr)
                {
                    Type timeOnlyType = targetType;
                    MethodInfo parseMethod = timeOnlyType.GetMethod("Parse", new[] { typeof(string), typeof(IFormatProvider) });
                    return parseMethod.Invoke(null, new object[] { timeStr, CultureInfo.InvariantCulture });
                }
            }

            // TimeSpan handling
            if (targetType == typeof(TimeSpan))
            {
                if (value is string tsStr)
                {
                    return TimeSpan.ParseExact(tsStr, "c", CultureInfo.InvariantCulture);
                }
                return TimeSpan.Parse(value.ToString(), CultureInfo.InvariantCulture);
            }

            // Guid handling
            if (targetType == typeof(Guid))
            {
                if (value is string guidStr)
                {
                    return Guid.Parse(guidStr);
                }
                return new Guid(value.ToString());
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

        public bool CanConvert(Type type)
        {
            // We can convert most types
            return true;
        }

        public string GetDatabaseTypeString(Type type, PropertyInfo propertyInfo = null)
        {
            // Nullable handling
            type = Nullable.GetUnderlyingType(type) ?? type;

            // Check for PropertyAttribute
            PropertyAttribute attr = propertyInfo?.GetCustomAttribute<PropertyAttribute>();
            if (attr != null && (attr.PropertyFlags & Flags.String) == Flags.String)
            {
                return $"TEXT";
            }

            // Type mappings for SQLite
            if (type == typeof(bool))
                return "INTEGER";
            if (type == typeof(byte) || type == typeof(sbyte))
                return "INTEGER";
            if (type == typeof(short) || type == typeof(ushort))
                return "INTEGER";
            if (type == typeof(int) || type == typeof(uint))
                return "INTEGER";
            if (type == typeof(long) || type == typeof(ulong))
                return "INTEGER";
            if (type == typeof(float))
                return "REAL";
            if (type == typeof(double))
                return "REAL";
            if (type == typeof(decimal))
                return "REAL";
            if (type == typeof(DateTime) || type == typeof(DateTimeOffset))
                return "TEXT";
            if (type.Name == "DateOnly" || type.Name == "TimeOnly")
                return "TEXT";
            if (type == typeof(TimeSpan))
                return "TEXT";
            if (type == typeof(Guid))
                return "TEXT";
            if (type == typeof(string))
                return "TEXT";
            if (type.IsEnum)
            {
                if (attr != null && (attr.PropertyFlags & Flags.String) != Flags.String)
                    return "INTEGER";
                return "TEXT";
            }
            if (type.IsArray || (type.IsGenericType && typeof(IEnumerable).IsAssignableFrom(type)))
                return "TEXT";
            
            // Complex types stored as JSON
            return "TEXT";
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