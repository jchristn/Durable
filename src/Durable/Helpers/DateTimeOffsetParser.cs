namespace Durable.Helpers
{
    using System;
    using System.Globalization;
    using System.Linq;

    /// <summary>
    /// A comprehensive DateTimeOffset parser that handles various database and standard datetime formats
    /// while preserving timezone offset information and microsecond precision where available.
    /// Unlike DateTime, DateTimeOffset can preserve the original timezone offset from the input string.
    /// </summary>
    public class DateTimeOffsetParser
    {
#pragma warning disable CS8625 // Cannot convert null literal to non-nullable reference type.

        private static string[] _defaultFormats = new[]
        {
            // ========== 7-digit precision (SQL Server datetime2(7)) ==========
            "yyyy-MM-dd HH:mm:ss.fffffff",
            "yyyy-MM-ddTHH:mm:ss.fffffff",
            "yyyy-MM-dd HH:mm:ss.fffffffZ",
            "yyyy-MM-ddTHH:mm:ss.fffffffZ",
            "yyyy-MM-dd HH:mm:ss.fffffffzzz",
            "yyyy-MM-ddTHH:mm:ss.fffffffzzz",
            "yyyy-MM-dd HH:mm:ss.fffffffK",
            "yyyy-MM-ddTHH:mm:ss.fffffffK",

            // ========== 6-digit precision (microseconds) ==========
            "yyyy-MM-dd HH:mm:ss.ffffff",
            "yyyy-MM-ddTHH:mm:ss.ffffff",
            "yyyy-MM-dd HH:mm:ss.ffffffZ",
            "yyyy-MM-ddTHH:mm:ss.ffffffZ",
            "yyyy-MM-dd HH:mm:ss.ffffffzzz",
            "yyyy-MM-ddTHH:mm:ss.ffffffzzz",
            "yyyy-MM-dd HH:mm:ss.ffffffK",
            "yyyy-MM-ddTHH:mm:ss.ffffffK",
            "dd-MMM-yy hh.mm.ss.ffffff tt",  // Oracle with microseconds
            "dd-MMM-yy HH.mm.ss.ffffff",
            "dd/MM/yyyy HH:mm:ss.ffffff",
            "MM/dd/yyyy HH:mm:ss.ffffff",
            "yyyy/MM/dd HH:mm:ss.ffffff",

            // ========== 5-digit precision ==========
            "yyyy-MM-dd HH:mm:ss.fffff",
            "yyyy-MM-ddTHH:mm:ss.fffff",
            "yyyy-MM-dd HH:mm:ss.fffffZ",
            "yyyy-MM-ddTHH:mm:ss.fffffZ",
            "yyyy-MM-dd HH:mm:ss.fffffzzz",
            "yyyy-MM-ddTHH:mm:ss.fffffzzz",

            // ========== 4-digit precision ==========
            "yyyy-MM-dd HH:mm:ss.ffff",
            "yyyy-MM-ddTHH:mm:ss.ffff",
            "yyyy-MM-dd HH:mm:ss.ffffZ",
            "yyyy-MM-ddTHH:mm:ss.ffffZ",
            "yyyy-MM-dd HH:mm:ss.ffffzzz",
            "yyyy-MM-ddTHH:mm:ss.ffffzzz",

            // ========== 3-digit precision (milliseconds) ==========
            "yyyy-MM-dd HH:mm:ss.fff",
            "yyyy-MM-ddTHH:mm:ss.fff",
            "yyyy-MM-dd HH:mm:ss.fffZ",
            "yyyy-MM-ddTHH:mm:ss.fffZ",
            "yyyy-MM-dd HH:mm:ss.fffzzz",
            "yyyy-MM-ddTHH:mm:ss.fffzzz",
            "yyyy-MM-dd HH:mm:ss.fffK",
            "yyyy-MM-ddTHH:mm:ss.fffK",
            "MMM dd yyyy hh:mm:ss:ffftt",    // SQL Server with milliseconds
            "MMM dd yyyy  h:mm:ss:ffftt",    // SQL Server with double space and single-digit hour
            "MMM dd yyyy HH:mm:ss:fff",
            "MM/dd/yyyy HH:mm:ss.fff",
            "dd/MM/yyyy HH:mm:ss.fff",
            "yyyy/MM/dd HH:mm:ss.fff",
            "yyyy-MM-dd'T'HH:mm:ss.fff'Z'",  // RFC 3339

            // ========== 2-digit precision (centiseconds) ==========
            "yyyy-MM-dd HH:mm:ss.ff",
            "yyyy-MM-ddTHH:mm:ss.ff",
            "yyyy-MM-dd HH:mm:ss.ffZ",
            "yyyy-MM-ddTHH:mm:ss.ffZ",
            "yyyy-MM-dd HH:mm:ss.ffzzz",
            "yyyy-MM-ddTHH:mm:ss.ffzzz",

            // ========== 1-digit precision (deciseconds) ==========
            "yyyy-MM-dd HH:mm:ss.f",
            "yyyy-MM-ddTHH:mm:ss.f",
            "yyyy-MM-dd HH:mm:ss.fZ",
            "yyyy-MM-ddTHH:mm:ss.fZ",
            "yyyy-MM-dd HH:mm:ss.fzzz",
            "yyyy-MM-ddTHH:mm:ss.fzzz",

            // ========== Seconds precision with timezone ==========
            "yyyy-MM-dd HH:mm:ssZ",
            "yyyy-MM-ddTHH:mm:ssZ",
            "yyyy-MM-dd HH:mm:sszzz",
            "yyyy-MM-ddTHH:mm:sszzz",
            "yyyy-MM-dd HH:mm:ssK",
            "yyyy-MM-ddTHH:mm:ssK",

            // ========== Seconds precision without timezone ==========
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-ddTHH:mm:ss",
            "yyyy/MM/dd HH:mm:ss",
            "yyyy.MM.dd HH:mm:ss",
            "dd/MM/yyyy HH:mm:ss",
            "MM/dd/yyyy HH:mm:ss",
            "dd.MM.yyyy HH:mm:ss",
            "dd-MM-yyyy HH:mm:ss",
            "dd-MMM-yyyy HH:mm:ss",
            "yyyy-MM-dd h:mm:ss tt",         // 12-hour with AM/PM
            "yyyy-MM-dd hh:mm:ss tt",
            "MM/dd/yyyy h:mm:ss tt",
            "dd/MM/yyyy h:mm:ss tt",
            "M/d/yyyy h:mm:ss tt",
            "d/M/yyyy H:mm:ss",

            // ========== Compact formats with time ==========
            "yyyyMMddTHHmmssZ",              // ISO 8601 compact with Z
            "yyyyMMddTHHmmss",               // ISO 8601 compact with T
            "yyyyMMddHHmmss",                // MySQL compact
            "yyyyMMdd HH:mm:ss",

            // ========== Minutes precision ==========
            "yyyy-MM-dd HH:mm",
            "yyyy-MM-ddTHH:mm",
            "yyyy/MM/dd HH:mm",
            "MM/dd/yyyy HH:mm",
            "dd/MM/yyyy HH:mm",

            // ========== Date only formats (least precise) ==========
            "yyyy-MM-dd",
            "yyyy/MM/dd",
            "MM/dd/yyyy",
            "dd/MM/yyyy",
            "dd-MMM-yyyy",
            "dd-MMM-yy",
            "yyyyMMdd",
            "yyyy.MM.dd",
            "dd.MM.yyyy",

            // ========== Special/Legacy formats ==========
            "ddd, dd MMM yyyy HH:mm:ss zzz", // RFC 1123 with timezone
            "ddd, dd MMM yyyy HH:mm:ss",     // RFC 1123
            "G",                             // General date/time
            "s",                             // Sortable
            "u",                             // Universal sortable
            "o",                             // Round-trip
            "r"                              // RFC1123
        };

        private string[] _formats = null;
        private TimeSpan _defaultOffset = TimeSpan.Zero;

        /// <summary>
        /// Gets or sets the default timezone offset to use when parsing datetime strings without timezone information.
        /// Defaults to UTC (TimeSpan.Zero). Set this to your local offset or any other offset as needed.
        /// </summary>
        /// <remarks>
        /// When a datetime string has no timezone indicator (e.g., "2024-01-15 14:30:45"), this offset will be applied.
        /// For UTC, use TimeSpan.Zero (default).
        /// For local time, use TimeSpan.FromHours(-8) for PST, TimeSpan.FromHours(5) for EST+DST, etc.
        /// </remarks>
        public TimeSpan DefaultOffset
        {
            get => _defaultOffset;
            set => _defaultOffset = value;
        }

        /// <summary>
        /// Gets or sets the array of datetime format strings used for parsing.
        /// Formats are ordered from most precise (7-digit microseconds) to least precise (date only).
        /// Supports formats from MySQL, SQLite, SQL Server, Oracle, and PostgreSQL databases.
        /// Setting to null or empty array will revert to default formats.
        /// </summary>
        public string[] Formats
        {
            get => _formats ?? _defaultFormats;
            set => _formats = value;
        }

        /// <summary>
        /// Gets the default format strings used for parsing datetime values across various database systems.
        /// </summary>
        public static string[] DefaultFormats => (string[])_defaultFormats.Clone();

        /// <summary>
        /// Parses a datetime string using the configured format list, preserving precision and timezone offset.
        /// </summary>
        /// <param name="input">The datetime string to parse.</param>
        /// <returns>A DateTimeOffset object representing the parsed value with timezone offset preserved.</returns>
        /// <exception cref="ArgumentNullException">Thrown when input is null.</exception>
        /// <exception cref="FormatException">Thrown when the input string cannot be parsed using any of the configured formats.</exception>
        public static DateTimeOffset ParseString(string input)
        {
            return ParseString(input, _defaultFormats, TimeSpan.Zero);
        }

        /// <summary>
        /// Parses a datetime string using a custom format list, preserving precision and timezone offset.
        /// </summary>
        /// <param name="input">The datetime string to parse.</param>
        /// <param name="formats">Custom array of datetime format strings to use for parsing. If null or empty, uses default formats.</param>
        /// <param name="defaultOffset">The timezone offset to use when the input has no timezone information.</param>
        /// <returns>A DateTimeOffset object representing the parsed value.</returns>
        /// <exception cref="ArgumentNullException">Thrown when input is null.</exception>
        /// <exception cref="FormatException">Thrown when the input string cannot be parsed using any of the provided formats.</exception>
        public static DateTimeOffset ParseString(string input, string[] formats, TimeSpan defaultOffset)
        {
            if (input == null)
                throw new ArgumentNullException(nameof(input));

            // Use default formats if null or empty array provided
            if (formats == null || formats.Length == 0)
                formats = _defaultFormats;

            string dateStr = input.Trim();

            // Handle numeric formats first (Unix timestamps, ticks)
            // Only consider strings with exactly 10, 13, or 18+ digits as numeric timestamps
            // to avoid misidentifying compact datetime formats like "20240115"
            if (long.TryParse(dateStr, out long numericValue))
            {
                int digitCount = dateStr.Length;

                // .NET Ticks (18+ digits) - convert to DateTimeOffset with specified offset
                if (digitCount >= 18 && numericValue > 100000000000000000L)
                {
                    DateTime dt = new DateTime(numericValue, DateTimeKind.Unspecified);
                    return new DateTimeOffset(dt, defaultOffset);
                }
                // Unix timestamp in seconds (10 digits) - always UTC
                else if (digitCount == 10 && numericValue >= 1000000000L && numericValue < 10000000000L)
                {
                    return DateTimeOffset.FromUnixTimeSeconds(numericValue);
                }
                // Unix timestamp in milliseconds (13 digits) - always UTC
                else if (digitCount == 13 && numericValue >= 1000000000000L)
                {
                    return DateTimeOffset.FromUnixTimeMilliseconds(numericValue);
                }
                // Otherwise, let it fall through to format parsing
            }

            // Check if input has explicit timezone indicator at the END of the string
            // This avoids false positives from date separators like "2024-01-15"
            bool hasTimezone = System.Text.RegularExpressions.Regex.IsMatch(dateStr, @"(Z|[+-]\d{1,2}:?\d{2})$");

            // Try parsing with explicit formats
            if (hasTimezone)
            {
                // Input has timezone - use DateTimeOffset parsing to preserve it
                if (DateTimeOffset.TryParseExact(dateStr,
                    formats,
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.AllowWhiteSpaces,
                    out DateTimeOffset result))
                {
                    return result;
                }

                if (DateTimeOffset.TryParseExact(dateStr,
                    formats,
                    CultureInfo.CurrentCulture,
                    DateTimeStyles.AllowWhiteSpaces,
                    out result))
                {
                    return result;
                }
            }
            else
            {
                // No timezone in input - parse as DateTime, then apply default offset
                if (DateTime.TryParseExact(dateStr,
                    formats,
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.AllowWhiteSpaces,
                    out DateTime dt))
                {
                    // Create a new DateTime with Unspecified kind to ensure DateTimeOffset uses our offset
                    // This is more robust than SpecifyKind as it ensures no residual local time behavior
                    DateTime unspecDt = new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second, dt.Millisecond, DateTimeKind.Unspecified);
                    unspecDt = unspecDt.AddTicks(dt.Ticks % 10000); // Preserve sub-millisecond precision
                    return new DateTimeOffset(unspecDt, defaultOffset);
                }

                if (DateTime.TryParseExact(dateStr,
                    formats,
                    CultureInfo.CurrentCulture,
                    DateTimeStyles.AllowWhiteSpaces,
                    out dt))
                {
                    // Create a new DateTime with Unspecified kind
                    DateTime unspecDt = new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second, dt.Millisecond, DateTimeKind.Unspecified);
                    unspecDt = unspecDt.AddTicks(dt.Ticks % 10000); // Preserve sub-millisecond precision
                    return new DateTimeOffset(unspecDt, defaultOffset);
                }
            }

            // Handle special Oracle formats with custom parsing
            if (dateStr.Contains(".") && dateStr.Split(' ').Length >= 2)
            {
                // Oracle uses periods instead of colons in some formats
                string normalizedDate = dateStr.Replace('.', ':');
                if (hasTimezone)
                {
                    if (DateTimeOffset.TryParse(normalizedDate, out DateTimeOffset oracleResult))
                    {
                        return oracleResult;
                    }
                }
                else
                {
                    if (DateTime.TryParse(normalizedDate, out DateTime oracleDt))
                    {
                        DateTime unspecDt = new DateTime(oracleDt.Year, oracleDt.Month, oracleDt.Day, oracleDt.Hour, oracleDt.Minute, oracleDt.Second, oracleDt.Millisecond, DateTimeKind.Unspecified);
                        unspecDt = unspecDt.AddTicks(oracleDt.Ticks % 10000);
                        return new DateTimeOffset(unspecDt, defaultOffset);
                    }
                }
            }

            // Last resort - let built-in parsing try
            if (hasTimezone)
            {
                // Has timezone - use DateTimeOffset.Parse
                try
                {
                    return DateTimeOffset.Parse(dateStr, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces);
                }
                catch
                {
                    try
                    {
                        return DateTimeOffset.Parse(dateStr, CultureInfo.CurrentCulture, DateTimeStyles.AllowWhiteSpaces);
                    }
                    catch
                    {
                        throw new FormatException($"Unable to parse '{input}' as a valid DateTimeOffset using any of the {formats.Length} configured formats.");
                    }
                }
            }
            else
            {
                // No timezone - parse as DateTime and apply default offset
                try
                {
                    DateTime dt = DateTime.Parse(dateStr, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces);
                    DateTime unspecDt = new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second, dt.Millisecond, DateTimeKind.Unspecified);
                    unspecDt = unspecDt.AddTicks(dt.Ticks % 10000);
                    return new DateTimeOffset(unspecDt, defaultOffset);
                }
                catch
                {
                    try
                    {
                        DateTime dt = DateTime.Parse(dateStr, CultureInfo.CurrentCulture, DateTimeStyles.AllowWhiteSpaces);
                        DateTime unspecDt = new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second, dt.Millisecond, DateTimeKind.Unspecified);
                        unspecDt = unspecDt.AddTicks(dt.Ticks % 10000);
                        return new DateTimeOffset(unspecDt, defaultOffset);
                    }
                    catch
                    {
                        throw new FormatException($"Unable to parse '{input}' as a valid DateTimeOffset using any of the {formats.Length} configured formats.");
                    }
                }
            }
        }

        /// <summary>
        /// Attempts to parse a datetime string using the configured format list.
        /// </summary>
        /// <param name="input">The datetime string to parse.</param>
        /// <param name="result">When this method returns, contains the DateTimeOffset value equivalent to the date and time contained in input, if the conversion succeeded, or DateTimeOffset.MinValue if the conversion failed.</param>
        /// <returns>true if the input was converted successfully; otherwise, false.</returns>
        public static bool TryParseString(string input, out DateTimeOffset result)
        {
            return TryParseString(input, _defaultFormats, TimeSpan.Zero, out result);
        }

        /// <summary>
        /// Attempts to parse a datetime string using a custom format list.
        /// </summary>
        /// <param name="input">The datetime string to parse.</param>
        /// <param name="formats">Custom array of datetime format strings to use for parsing. If null or empty, uses default formats.</param>
        /// <param name="defaultOffset">The timezone offset to use when the input has no timezone information.</param>
        /// <param name="result">When this method returns, contains the DateTimeOffset value equivalent to the date and time contained in input, if the conversion succeeded, or DateTimeOffset.MinValue if the conversion failed.</param>
        /// <returns>true if the input was converted successfully; otherwise, false.</returns>
        public static bool TryParseString(string input, string[] formats, TimeSpan defaultOffset, out DateTimeOffset result)
        {
            result = DateTimeOffset.MinValue;

            if (string.IsNullOrEmpty(input))
                return false;

            // Use default formats if null or empty array provided
            if (formats == null || formats.Length == 0)
                formats = _defaultFormats;

            try
            {
                result = ParseString(input, formats, defaultOffset);
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Parses a datetime string using this instance's configured format list and default offset.
        /// </summary>
        /// <param name="input">The datetime string to parse.</param>
        /// <returns>A DateTimeOffset object representing the parsed value.</returns>
        /// <exception cref="ArgumentNullException">Thrown when input is null.</exception>
        /// <exception cref="FormatException">Thrown when the input string cannot be parsed using any of the configured formats.</exception>
        public DateTimeOffset Parse(string input)
        {
            return ParseString(input, this.Formats, this.DefaultOffset);
        }

        /// <summary>
        /// Attempts to parse a datetime string using this instance's configured format list and default offset.
        /// </summary>
        /// <param name="input">The datetime string to parse.</param>
        /// <param name="result">When this method returns, contains the DateTimeOffset value equivalent to the date and time contained in input, if the conversion succeeded, or DateTimeOffset.MinValue if the conversion failed.</param>
        /// <returns>true if the input was converted successfully; otherwise, false.</returns>
        public bool TryParse(string input, out DateTimeOffset result)
        {
            return TryParseString(input, this.Formats, this.DefaultOffset, out result);
        }

        /// <summary>
        /// Resets the instance formats to use the default format list and resets the default offset to UTC.
        /// </summary>
        public void ResetToDefaults()
        {
            _formats = null;
            _defaultOffset = TimeSpan.Zero;
        }

#pragma warning restore CS8625 // Cannot convert null literal to non-nullable reference type.
    }
}
