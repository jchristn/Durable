namespace Durable
{
    using System;
    using System.Collections.Generic;
    using System.Linq.Expressions;

    /// <summary>
    /// Provides extension methods for various expression operations and value comparisons.
    /// </summary>
    public static class ExpressionExtensions
    {
        /// <summary>
        /// Determines whether a value falls between two bounds (inclusive).
        /// </summary>
        /// <typeparam name="T">The type of the values being compared.</typeparam>
        /// <param name="value">The value to check.</param>
        /// <param name="min">The minimum bound.</param>
        /// <param name="max">The maximum bound.</param>
        /// <returns>true if the value is between min and max (inclusive); otherwise, false.</returns>
        public static bool Between<T>(this T value, T min, T max) where T : IComparable<T>
        {
            return value.CompareTo(min) >= 0 && value.CompareTo(max) <= 0;
        }

        /// <summary>
        /// Determines whether an integer value falls between two bounds (inclusive).
        /// </summary>
        /// <param name="value">The integer value to check.</param>
        /// <param name="min">The minimum bound.</param>
        /// <param name="max">The maximum bound.</param>
        /// <returns>true if the value is between min and max (inclusive); otherwise, false.</returns>
        public static bool Between(this int value, int min, int max)
        {
            return value >= min && value <= max;
        }

        /// <summary>
        /// Determines whether a decimal value falls between two bounds (inclusive).
        /// </summary>
        /// <param name="value">The decimal value to check.</param>
        /// <param name="min">The minimum bound.</param>
        /// <param name="max">The maximum bound.</param>
        /// <returns>true if the value is between min and max (inclusive); otherwise, false.</returns>
        public static bool Between(this decimal value, decimal min, decimal max)
        {
            return value >= min && value <= max;
        }

        /// <summary>
        /// Determines whether a double value falls between two bounds (inclusive).
        /// </summary>
        /// <param name="value">The double value to check.</param>
        /// <param name="min">The minimum bound.</param>
        /// <param name="max">The maximum bound.</param>
        /// <returns>true if the value is between min and max (inclusive); otherwise, false.</returns>
        public static bool Between(this double value, double min, double max)
        {
            return value >= min && value <= max;
        }

        /// <summary>
        /// Determines whether a float value falls between two bounds (inclusive).
        /// </summary>
        /// <param name="value">The float value to check.</param>
        /// <param name="min">The minimum bound.</param>
        /// <param name="max">The maximum bound.</param>
        /// <returns>true if the value is between min and max (inclusive); otherwise, false.</returns>
        public static bool Between(this float value, float min, float max)
        {
            return value >= min && value <= max;
        }

        /// <summary>
        /// Determines whether a DateTime value falls between two bounds (inclusive).
        /// </summary>
        /// <param name="value">The DateTime value to check.</param>
        /// <param name="min">The minimum bound.</param>
        /// <param name="max">The maximum bound.</param>
        /// <returns>true if the value is between min and max (inclusive); otherwise, false.</returns>
        public static bool Between(this DateTime value, DateTime min, DateTime max)
        {
            return value >= min && value <= max;
        }

        /// <summary>
        /// Determines whether a DateTimeOffset value falls between two bounds (inclusive).
        /// </summary>
        /// <param name="value">The DateTimeOffset value to check.</param>
        /// <param name="min">The minimum bound.</param>
        /// <param name="max">The maximum bound.</param>
        /// <returns>true if the value is between min and max (inclusive); otherwise, false.</returns>
        public static bool Between(this DateTimeOffset value, DateTimeOffset min, DateTimeOffset max)
        {
            return value >= min && value <= max;
        }

        /// <summary>
        /// Determines whether a value exists in the specified array of values.
        /// </summary>
        /// <typeparam name="T">The type of the values.</typeparam>
        /// <param name="value">The value to search for.</param>
        /// <param name="values">The array of values to search in.</param>
        /// <returns>true if the value is found in the array; otherwise, false.</returns>
        /// <exception cref="ArgumentNullException">Thrown when values is null.</exception>
        public static bool In<T>(this T value, params T[] values)
        {
            if (values == null) throw new ArgumentNullException(nameof(values));
            return Array.IndexOf(values, value) >= 0;
        }

        /// <summary>
        /// Determines whether a value exists in the specified collection of values.
        /// </summary>
        /// <typeparam name="T">The type of the values.</typeparam>
        /// <param name="value">The value to search for.</param>
        /// <param name="values">The collection of values to search in.</param>
        /// <returns>true if the value is found in the collection; otherwise, false.</returns>
        /// <exception cref="ArgumentNullException">Thrown when values is null.</exception>
        public static bool In<T>(this T value, IEnumerable<T> values)
        {
            if (values == null) throw new ArgumentNullException(nameof(values));
            return values.Contains(value);
        }

        /// <summary>
        /// Determines whether a value does not exist in the specified array of values.
        /// </summary>
        /// <typeparam name="T">The type of the values.</typeparam>
        /// <param name="value">The value to search for.</param>
        /// <param name="values">The array of values to search in.</param>
        /// <returns>true if the value is not found in the array; otherwise, false.</returns>
        /// <exception cref="ArgumentNullException">Thrown when values is null.</exception>
        public static bool NotIn<T>(this T value, params T[] values)
        {
            if (values == null) throw new ArgumentNullException(nameof(values));
            return Array.IndexOf(values, value) < 0;
        }

        /// <summary>
        /// Determines whether a value does not exist in the specified collection of values.
        /// </summary>
        /// <typeparam name="T">The type of the values.</typeparam>
        /// <param name="value">The value to search for.</param>
        /// <param name="values">The collection of values to search in.</param>
        /// <returns>true if the value is not found in the collection; otherwise, false.</returns>
        /// <exception cref="ArgumentNullException">Thrown when values is null.</exception>
        public static bool NotIn<T>(this T value, IEnumerable<T> values)
        {
            if (values == null) throw new ArgumentNullException(nameof(values));
            return !values.Contains(value);
        }

        /// <summary>
        /// Determines whether a nullable value type is null.
        /// </summary>
        /// <typeparam name="T">The underlying value type.</typeparam>
        /// <param name="value">The nullable value to check.</param>
        /// <returns>true if the value is null; otherwise, false.</returns>
        public static bool IsNull<T>(this T? value) where T : struct
        {
            return !value.HasValue;
        }

        /// <summary>
        /// Determines whether a nullable value type is not null.
        /// </summary>
        /// <typeparam name="T">The underlying value type.</typeparam>
        /// <param name="value">The nullable value to check.</param>
        /// <returns>true if the value is not null; otherwise, false.</returns>
        public static bool IsNotNull<T>(this T? value) where T : struct
        {
            return value.HasValue;
        }

        /// <summary>
        /// Determines whether a string is null.
        /// </summary>
        /// <param name="value">The string to check.</param>
        /// <returns>true if the string is null; otherwise, false.</returns>
        public static bool IsNull(this string value)
        {
            return value == null;
        }

        /// <summary>
        /// Determines whether a string is not null.
        /// </summary>
        /// <param name="value">The string to check.</param>
        /// <returns>true if the string is not null; otherwise, false.</returns>
        public static bool IsNotNull(this string value)
        {
            return value != null;
        }

        /// <summary>
        /// Determines whether a string is null or empty.
        /// </summary>
        /// <param name="value">The string to check.</param>
        /// <returns>true if the string is null or empty; otherwise, false.</returns>
        public static bool IsNullOrEmpty(this string value)
        {
            return string.IsNullOrEmpty(value);
        }

        /// <summary>
        /// Determines whether a string is not null or empty.
        /// </summary>
        /// <param name="value">The string to check.</param>
        /// <returns>true if the string is not null or empty; otherwise, false.</returns>
        public static bool IsNotNullOrEmpty(this string value)
        {
            return !string.IsNullOrEmpty(value);
        }

        /// <summary>
        /// Determines whether a string is null, empty, or consists only of white-space characters.
        /// </summary>
        /// <param name="value">The string to check.</param>
        /// <returns>true if the string is null, empty, or contains only white-space characters; otherwise, false.</returns>
        public static bool IsNullOrWhiteSpace(this string value)
        {
            return string.IsNullOrWhiteSpace(value);
        }

        /// <summary>
        /// Determines whether a string is not null, empty, or consists only of white-space characters.
        /// </summary>
        /// <param name="value">The string to check.</param>
        /// <returns>true if the string is not null, empty, or contains only white-space characters; otherwise, false.</returns>
        public static bool IsNotNullOrWhiteSpace(this string value)
        {
            return !string.IsNullOrWhiteSpace(value);
        }
    }
}