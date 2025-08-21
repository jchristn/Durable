namespace Durable
{
    using System;
    using System.Collections.Generic;
    using System.Linq.Expressions;

    public static class ExpressionExtensions
    {
        public static bool Between<T>(this T value, T min, T max) where T : IComparable<T>
        {
            return value.CompareTo(min) >= 0 && value.CompareTo(max) <= 0;
        }

        public static bool Between(this int value, int min, int max)
        {
            return value >= min && value <= max;
        }

        public static bool Between(this decimal value, decimal min, decimal max)
        {
            return value >= min && value <= max;
        }

        public static bool Between(this double value, double min, double max)
        {
            return value >= min && value <= max;
        }

        public static bool Between(this float value, float min, float max)
        {
            return value >= min && value <= max;
        }

        public static bool Between(this DateTime value, DateTime min, DateTime max)
        {
            return value >= min && value <= max;
        }

        public static bool Between(this DateTimeOffset value, DateTimeOffset min, DateTimeOffset max)
        {
            return value >= min && value <= max;
        }

        public static bool In<T>(this T value, params T[] values)
        {
            return Array.IndexOf(values, value) >= 0;
        }

        public static bool In<T>(this T value, IEnumerable<T> values)
        {
            return values.Contains(value);
        }

        public static bool NotIn<T>(this T value, params T[] values)
        {
            return Array.IndexOf(values, value) < 0;
        }

        public static bool NotIn<T>(this T value, IEnumerable<T> values)
        {
            return !values.Contains(value);
        }

        public static bool IsNull<T>(this T? value) where T : struct
        {
            return !value.HasValue;
        }

        public static bool IsNotNull<T>(this T? value) where T : struct
        {
            return value.HasValue;
        }

        public static bool IsNull(this string value)
        {
            return value == null;
        }

        public static bool IsNotNull(this string value)
        {
            return value != null;
        }

        public static bool IsNullOrEmpty(this string value)
        {
            return string.IsNullOrEmpty(value);
        }

        public static bool IsNotNullOrEmpty(this string value)
        {
            return !string.IsNullOrEmpty(value);
        }

        public static bool IsNullOrWhiteSpace(this string value)
        {
            return string.IsNullOrWhiteSpace(value);
        }

        public static bool IsNotNullOrWhiteSpace(this string value)
        {
            return !string.IsNullOrWhiteSpace(value);
        }
    }
}