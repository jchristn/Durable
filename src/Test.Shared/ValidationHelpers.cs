namespace Test.Shared
{
    using System;

    /// <summary>
    /// Provides precise validation and comparison methods for test assertions.
    /// Ensures exact matches for all data types including DateTime precision, enums, booleans, strings, and numbers.
    /// </summary>
    public static class ValidationHelpers
    {
        #region Public-Methods

        /// <summary>
        /// Validates that two DateTime values are equal with millisecond precision.
        /// Different databases may store DateTime with different precision levels.
        /// </summary>
        /// <param name="expected">The expected DateTime value.</param>
        /// <param name="actual">The actual DateTime value.</param>
        /// <param name="precisionMs">The precision in milliseconds. Default is 1000ms (1 second).</param>
        /// <returns>True if the values are equal within the specified precision.</returns>
        public static bool AreDateTimesEqual(DateTime expected, DateTime actual, int precisionMs = 1000)
        {
            TimeSpan difference = expected > actual ? expected - actual : actual - expected;
            return difference.TotalMilliseconds < precisionMs;
        }

        /// <summary>
        /// Validates that two nullable DateTime values are equal with millisecond precision.
        /// </summary>
        /// <param name="expected">The expected nullable DateTime value.</param>
        /// <param name="actual">The actual nullable DateTime value.</param>
        /// <param name="precisionMs">The precision in milliseconds. Default is 1000ms (1 second).</param>
        /// <returns>True if the values are equal within the specified precision.</returns>
        public static bool AreDateTimesEqual(DateTime? expected, DateTime? actual, int precisionMs = 1000)
        {
            if (expected == null && actual == null) return true;
            if (expected == null || actual == null) return false;
            return AreDateTimesEqual(expected.Value, actual.Value, precisionMs);
        }

        /// <summary>
        /// Validates that two DateTimeOffset values are equal with millisecond precision.
        /// </summary>
        /// <param name="expected">The expected DateTimeOffset value.</param>
        /// <param name="actual">The actual DateTimeOffset value.</param>
        /// <param name="precisionMs">The precision in milliseconds. Default is 1000ms (1 second).</param>
        /// <returns>True if the values are equal within the specified precision.</returns>
        public static bool AreDateTimeOffsetsEqual(DateTimeOffset expected, DateTimeOffset actual, int precisionMs = 1000)
        {
            TimeSpan difference = expected > actual ? expected - actual : actual - expected;
            return difference.TotalMilliseconds < precisionMs;
        }

        /// <summary>
        /// Validates that two nullable DateTimeOffset values are equal with millisecond precision.
        /// </summary>
        /// <param name="expected">The expected nullable DateTimeOffset value.</param>
        /// <param name="actual">The actual nullable DateTimeOffset value.</param>
        /// <param name="precisionMs">The precision in milliseconds. Default is 1000ms (1 second).</param>
        /// <returns>True if the values are equal within the specified precision.</returns>
        public static bool AreDateTimeOffsetsEqual(DateTimeOffset? expected, DateTimeOffset? actual, int precisionMs = 1000)
        {
            if (expected == null && actual == null) return true;
            if (expected == null || actual == null) return false;
            return AreDateTimeOffsetsEqual(expected.Value, actual.Value, precisionMs);
        }

        /// <summary>
        /// Validates that two enum values are exactly equal.
        /// </summary>
        /// <typeparam name="TEnum">The enum type.</typeparam>
        /// <param name="expected">The expected enum value.</param>
        /// <param name="actual">The actual enum value.</param>
        /// <returns>True if the enum values are exactly equal.</returns>
        public static bool AreEnumsEqual<TEnum>(TEnum expected, TEnum actual) where TEnum : struct, Enum
        {
            return expected.Equals(actual);
        }

        /// <summary>
        /// Validates that two boolean values are exactly equal.
        /// </summary>
        /// <param name="expected">The expected boolean value.</param>
        /// <param name="actual">The actual boolean value.</param>
        /// <returns>True if the boolean values are exactly equal.</returns>
        public static bool AreBooleansEqual(bool expected, bool actual)
        {
            return expected == actual;
        }

        /// <summary>
        /// Validates that two nullable boolean values are exactly equal.
        /// </summary>
        /// <param name="expected">The expected nullable boolean value.</param>
        /// <param name="actual">The actual nullable boolean value.</param>
        /// <returns>True if the boolean values are exactly equal.</returns>
        public static bool AreBooleansEqual(bool? expected, bool? actual)
        {
            return expected == actual;
        }

        /// <summary>
        /// Validates that two string values are exactly equal.
        /// </summary>
        /// <param name="expected">The expected string value.</param>
        /// <param name="actual">The actual string value.</param>
        /// <param name="caseSensitive">Whether the comparison should be case-sensitive. Default is true.</param>
        /// <returns>True if the string values are exactly equal.</returns>
        public static bool AreStringsEqual(string? expected, string? actual, bool caseSensitive = true)
        {
            if (expected == null && actual == null) return true;
            if (expected == null || actual == null) return false;

            if (caseSensitive)
            {
                return string.Equals(expected, actual, StringComparison.Ordinal);
            }
            else
            {
                return string.Equals(expected, actual, StringComparison.OrdinalIgnoreCase);
            }
        }

        /// <summary>
        /// Validates that two integer values are exactly equal.
        /// </summary>
        /// <param name="expected">The expected integer value.</param>
        /// <param name="actual">The actual integer value.</param>
        /// <returns>True if the integer values are exactly equal.</returns>
        public static bool AreIntegersEqual(int expected, int actual)
        {
            return expected == actual;
        }

        /// <summary>
        /// Validates that two nullable integer values are exactly equal.
        /// </summary>
        /// <param name="expected">The expected nullable integer value.</param>
        /// <param name="actual">The actual nullable integer value.</param>
        /// <returns>True if the integer values are exactly equal.</returns>
        public static bool AreIntegersEqual(int? expected, int? actual)
        {
            return expected == actual;
        }

        /// <summary>
        /// Validates that two long values are exactly equal.
        /// </summary>
        /// <param name="expected">The expected long value.</param>
        /// <param name="actual">The actual long value.</param>
        /// <returns>True if the long values are exactly equal.</returns>
        public static bool AreLongsEqual(long expected, long actual)
        {
            return expected == actual;
        }

        /// <summary>
        /// Validates that two decimal values are exactly equal.
        /// </summary>
        /// <param name="expected">The expected decimal value.</param>
        /// <param name="actual">The actual decimal value.</param>
        /// <returns>True if the decimal values are exactly equal.</returns>
        public static bool AreDecimalsEqual(decimal expected, decimal actual)
        {
            return expected == actual;
        }

        /// <summary>
        /// Validates that two nullable decimal values are exactly equal.
        /// </summary>
        /// <param name="expected">The expected nullable decimal value.</param>
        /// <param name="actual">The actual nullable decimal value.</param>
        /// <returns>True if the decimal values are exactly equal.</returns>
        public static bool AreDecimalsEqual(decimal? expected, decimal? actual)
        {
            return expected == actual;
        }

        /// <summary>
        /// Validates that two double values are equal within a specified tolerance.
        /// </summary>
        /// <param name="expected">The expected double value.</param>
        /// <param name="actual">The actual double value.</param>
        /// <param name="tolerance">The tolerance for comparison. Default is 0.0001.</param>
        /// <returns>True if the double values are equal within the tolerance.</returns>
        public static bool AreDoublesEqual(double expected, double actual, double tolerance = 0.0001)
        {
            return Math.Abs(expected - actual) < tolerance;
        }

        /// <summary>
        /// Validates that two Guid values are exactly equal.
        /// </summary>
        /// <param name="expected">The expected Guid value.</param>
        /// <param name="actual">The actual Guid value.</param>
        /// <returns>True if the Guid values are exactly equal.</returns>
        public static bool AreGuidsEqual(Guid expected, Guid actual)
        {
            return expected == actual;
        }

        /// <summary>
        /// Validates that two TimeSpan values are exactly equal.
        /// </summary>
        /// <param name="expected">The expected TimeSpan value.</param>
        /// <param name="actual">The actual TimeSpan value.</param>
        /// <returns>True if the TimeSpan values are exactly equal.</returns>
        public static bool AreTimeSpansEqual(TimeSpan expected, TimeSpan actual)
        {
            return expected == actual;
        }

        /// <summary>
        /// Formats a DateTime value for console output with consistent formatting.
        /// </summary>
        /// <param name="dateTime">The DateTime value to format.</param>
        /// <returns>A formatted string representation.</returns>
        public static string FormatDateTime(DateTime dateTime)
        {
            return dateTime.ToString("yyyy-MM-dd HH:mm:ss.fff");
        }

        /// <summary>
        /// Formats a nullable DateTime value for console output with consistent formatting.
        /// </summary>
        /// <param name="dateTime">The nullable DateTime value to format.</param>
        /// <returns>A formatted string representation or "NULL" if the value is null.</returns>
        public static string FormatDateTime(DateTime? dateTime)
        {
            return dateTime.HasValue ? FormatDateTime(dateTime.Value) : "NULL";
        }

        /// <summary>
        /// Formats a DateTimeOffset value for console output with consistent formatting.
        /// </summary>
        /// <param name="dateTimeOffset">The DateTimeOffset value to format.</param>
        /// <returns>A formatted string representation.</returns>
        public static string FormatDateTimeOffset(DateTimeOffset dateTimeOffset)
        {
            return dateTimeOffset.ToString("yyyy-MM-dd HH:mm:ss.fff zzz");
        }

        /// <summary>
        /// Formats a nullable DateTimeOffset value for console output with consistent formatting.
        /// </summary>
        /// <param name="dateTimeOffset">The nullable DateTimeOffset value to format.</param>
        /// <returns>A formatted string representation or "NULL" if the value is null.</returns>
        public static string FormatDateTimeOffset(DateTimeOffset? dateTimeOffset)
        {
            return dateTimeOffset.HasValue ? FormatDateTimeOffset(dateTimeOffset.Value) : "NULL";
        }

        #endregion
    }
}
