namespace Durable.Helpers
{
    using System;
    using System.Globalization;
    using System.Linq;

    /// <summary>
    /// A comprehensive DateTime parser that handles various database and standard datetime formats
    /// while preserving microsecond precision where available.
    /// </summary>
    public class DateTimeParser
    {
#pragma warning disable CS8625 // Cannot convert null literal to non-nullable reference type.

        private static string[] _defaultFormats = new[]
        {
            // ========== 7-digit precision (SQL Server datetime2(7)) ==========
            "yyyy-MM-dd HH:mm:ss.fffffff",
            "yyyy-MM-ddTHH:mm:ss.fffffff",
            "yyyy-MM-dd HH:mm:ss.fffffffZ",
            "yyyy-MM-ddTHH:mm:ss.fffffffZ",
            "yyyy-MM-dd HH:mm:ss.fffffff+00",
            "yyyy-MM-ddTHH:mm:ss.fffffff+00",
            "yyyy-MM-dd HH:mm:ss.fffffff-00",
            "yyyy-MM-ddTHH:mm:ss.fffffff-00",
            "yyyy-MM-dd HH:mm:ss.fffffffK",
            "yyyy-MM-ddTHH:mm:ss.fffffffK",

            // ========== 6-digit precision (microseconds) ==========
            "yyyy-MM-dd HH:mm:ss.ffffff",
            "yyyy-MM-ddTHH:mm:ss.ffffff",
            "yyyy-MM-dd HH:mm:ss.ffffffZ",
            "yyyy-MM-ddTHH:mm:ss.ffffffZ",
            "yyyy-MM-dd HH:mm:ss.ffffffzzz",
            "yyyy-MM-ddTHH:mm:ss.ffffffzzz",
            "yyyy-MM-dd HH:mm:ss.ffffff+00",
            "yyyy-MM-ddTHH:mm:ss.ffffff+00",
            "yyyy-MM-dd HH:mm:ss.ffffff-00",
            "yyyy-MM-ddTHH:mm:ss.ffffff-00",
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

            // ========== 4-digit precision ==========
            "yyyy-MM-dd HH:mm:ss.ffff",
            "yyyy-MM-ddTHH:mm:ss.ffff",
            "yyyy-MM-dd HH:mm:ss.ffffZ",
            "yyyy-MM-ddTHH:mm:ss.ffffZ",

            // ========== 3-digit precision (milliseconds) ==========
            "yyyy-MM-dd HH:mm:ss.fff",
            "yyyy-MM-ddTHH:mm:ss.fff",
            "yyyy-MM-dd HH:mm:ss.fffZ",
            "yyyy-MM-ddTHH:mm:ss.fffZ",
            "yyyy-MM-dd HH:mm:ss.fff+00",
            "yyyy-MM-ddTHH:mm:ss.fff+00",
            "yyyy-MM-dd HH:mm:ss.fff-00",
            "yyyy-MM-ddTHH:mm:ss.fff-00",
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

            // ========== 1-digit precision (deciseconds) ==========
            "yyyy-MM-dd HH:mm:ss.f",
            "yyyy-MM-ddTHH:mm:ss.f",
            "yyyy-MM-dd HH:mm:ss.fZ",
            "yyyy-MM-ddTHH:mm:ss.fZ",

            // ========== Seconds precision with timezone ==========
            "yyyy-MM-dd HH:mm:ssZ",
            "yyyy-MM-ddTHH:mm:ssZ",
            "yyyy-MM-dd HH:mm:sszzz",
            "yyyy-MM-ddTHH:mm:sszzz",
            "yyyy-MM-dd HH:mm:ss+00",
            "yyyy-MM-ddTHH:mm:ss+00",
            "yyyy-MM-dd HH:mm:ss-00",
            "yyyy-MM-ddTHH:mm:ss-00",
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
            "ddd, dd MMM yyyy HH:mm:ss",     // RFC 1123
            "G",                             // General date/time
            "s",                             // Sortable
            "u",                             // Universal sortable
            "o",                             // Round-trip
            "r"                              // RFC1123
        };

        private string[] _formats = null;

        /// <summary>
        /// Gets or sets the array of datetime format strings used for parsing.
        /// Formats are ordered from most precise (7-digit microseconds) to least precise (date only).
        /// Supports formats from MySQL, SQLite, SQL Server, Oracle, and PostgreSQL databases.
        /// Setting to null or empty array will revert to default formats.
        /// </summary>
        /// <remarks>
        /// The default format list includes:
        /// - ISO 8601 formats with various precision levels (up to 7 digits)
        /// - Timezone-aware formats (with Z, +00, -00, zzz, K suffixes)
        /// - Database-specific formats (Oracle with periods, MySQL compact, SQL Server with milliseconds)
        /// - Culture-specific formats (US, European, with/without AM/PM)
        /// - Unix timestamps and .NET ticks (handled separately in parsing logic)
        ///
        /// Custom format arrays can be assigned to optimize for specific database systems or requirements.
        /// Setting this property to null or an empty array will use the default formats.
        /// </remarks>
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
        /// Parses a datetime string using the configured format list, preserving precision up to microseconds.
        /// </summary>
        /// <param name="input">The datetime string to parse.</param>
        /// <returns>A DateTime object representing the parsed value.</returns>
        /// <exception cref="ArgumentNullException">Thrown when input is null.</exception>
        /// <exception cref="FormatException">Thrown when the input string cannot be parsed using any of the configured formats.</exception>
        public static DateTime ParseString(string input)
        {
            return ParseString(input, _defaultFormats);
        }

        /// <summary>
        /// Parses a datetime string using a custom format list, preserving precision up to microseconds.
        /// </summary>
        /// <param name="input">The datetime string to parse.</param>
        /// <param name="formats">Custom array of datetime format strings to use for parsing. If null or empty, uses default formats.</param>
        /// <returns>A DateTime object representing the parsed value.</returns>
        /// <exception cref="ArgumentNullException">Thrown when input is null.</exception>
        /// <exception cref="FormatException">Thrown when the input string cannot be parsed using any of the provided formats.</exception>
        public static DateTime ParseString(string input, string[] formats)
        {
            if (input == null)
                throw new ArgumentNullException(nameof(input));

            // Use default formats if null or empty array provided
            if (formats == null || formats.Length == 0)
                formats = _defaultFormats;

            string dateStr = input.Trim();

            // Handle numeric formats first (Unix timestamps, ticks)
            // Only consider strings with exactly 8, 10, 13, or 18+ digits as numeric timestamps
            // to avoid misidentifying compact datetime formats like "20240115"
            if (long.TryParse(dateStr, out long numericValue))
            {
                int digitCount = dateStr.Length;

                // .NET Ticks (18+ digits)
                if (digitCount >= 18 && numericValue > 100000000000000000L)
                {
                    return new DateTime(numericValue, DateTimeKind.Unspecified);
                }
                // Unix timestamp in seconds (10 digits)
                // Unix timestamps are ALWAYS UTC by definition
                else if (digitCount == 10 && numericValue >= 1000000000L && numericValue < 10000000000L)
                {
                    return DateTimeOffset.FromUnixTimeSeconds(numericValue).UtcDateTime;
                }
                // Unix timestamp in milliseconds (13 digits)
                // Unix timestamps are ALWAYS UTC by definition
                else if (digitCount == 13 && numericValue >= 1000000000000L)
                {
                    return DateTimeOffset.FromUnixTimeMilliseconds(numericValue).UtcDateTime;
                }
                // Otherwise, let it fall through to format parsing
            }

            // Try parsing with explicit formats, preserving timezone information
            if (DateTime.TryParseExact(dateStr,
                formats,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AllowWhiteSpaces | DateTimeStyles.RoundtripKind,
                out DateTime result))
            {
                // Post-process: If input has UTC indicators (+00, -00, +00:00, -00:00) but Kind is not UTC, fix it
                result = EnsureUtcKindForUtcIndicators(dateStr, result);
                return result;
            }

            // Try with current culture formats
            if (DateTime.TryParseExact(dateStr,
                formats,
                CultureInfo.CurrentCulture,
                DateTimeStyles.AllowWhiteSpaces | DateTimeStyles.RoundtripKind,
                out result))
            {
                // Post-process: If input has UTC indicators (+00, -00, +00:00, -00:00) but Kind is not UTC, fix it
                result = EnsureUtcKindForUtcIndicators(dateStr, result);
                return result;
            }

            // Handle special Oracle formats with custom parsing
            if (dateStr.Contains(".") && dateStr.Split(' ').Length >= 2)
            {
                // Oracle uses periods instead of colons in some formats
                string normalizedDate = dateStr.Replace('.', ':');
                if (DateTime.TryParse(normalizedDate, out result))
                {
                    return result;
                }
            }

            // Last resort - let DateTime.Parse try with its built-in intelligence
            try
            {
                return DateTime.Parse(dateStr, CultureInfo.InvariantCulture,
                    DateTimeStyles.AllowWhiteSpaces | DateTimeStyles.RoundtripKind);
            }
            catch
            {
                try
                {
                    // Final fallback with current culture
                    return DateTime.Parse(dateStr, CultureInfo.CurrentCulture,
                        DateTimeStyles.AllowWhiteSpaces | DateTimeStyles.RoundtripKind);
                }
                catch
                {
                    throw new FormatException($"Unable to parse '{input}' as a valid DateTime using any of the {formats.Length} configured formats.");
                }
            }
        }

        /// <summary>
        /// Attempts to parse a datetime string using the configured format list.
        /// </summary>
        /// <param name="input">The datetime string to parse.</param>
        /// <param name="result">When this method returns, contains the DateTime value equivalent to the date and time contained in input, if the conversion succeeded, or DateTime.MinValue if the conversion failed.</param>
        /// <returns>true if the input was converted successfully; otherwise, false.</returns>
        public static bool TryParseString(string input, out DateTime result)
        {
            return TryParseString(input, _defaultFormats, out result);
        }

        /// <summary>
        /// Attempts to parse a datetime string using a custom format list.
        /// </summary>
        /// <param name="input">The datetime string to parse.</param>
        /// <param name="formats">Custom array of datetime format strings to use for parsing. If null or empty, uses default formats.</param>
        /// <param name="result">When this method returns, contains the DateTime value equivalent to the date and time contained in input, if the conversion succeeded, or DateTime.MinValue if the conversion failed.</param>
        /// <returns>true if the input was converted successfully; otherwise, false.</returns>
        public static bool TryParseString(string input, string[] formats, out DateTime result)
        {
            result = DateTime.MinValue;

            if (string.IsNullOrEmpty(input))
                return false;

            // Use default formats if null or empty array provided
            if (formats == null || formats.Length == 0)
                formats = _defaultFormats;

            try
            {
                result = ParseString(input, formats);
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Parses a datetime string using this instance's configured format list.
        /// </summary>
        /// <param name="input">The datetime string to parse.</param>
        /// <returns>A DateTime object representing the parsed value.</returns>
        /// <exception cref="ArgumentNullException">Thrown when input is null.</exception>
        /// <exception cref="FormatException">Thrown when the input string cannot be parsed using any of the configured formats.</exception>
        public DateTime Parse(string input)
        {
            return ParseString(input, this.Formats);
        }

        /// <summary>
        /// Attempts to parse a datetime string using this instance's configured format list.
        /// </summary>
        /// <param name="input">The datetime string to parse.</param>
        /// <param name="result">When this method returns, contains the DateTime value equivalent to the date and time contained in input, if the conversion succeeded, or DateTime.MinValue if the conversion failed.</param>
        /// <returns>true if the input was converted successfully; otherwise, false.</returns>
        public bool TryParse(string input, out DateTime result)
        {
            return TryParseString(input, this.Formats, out result);
        }

        /// <summary>
        /// Resets the instance formats to use the default format list.
        /// </summary>
        public void ResetToDefaults()
        {
            _formats = null;
        }

        /// <summary>
        /// Post-processes a parsed DateTime to ensure that UTC indicators in the input string
        /// result in Kind=Utc, even when .NET's literal format matching doesn't set it.
        /// This handles cases like "+00", "-00", "+00:00", "-00:00" which are UTC but
        /// .NET's literal format matching sets Kind=Unspecified or converts to Local.
        /// </summary>
        private static DateTime EnsureUtcKindForUtcIndicators(string input, DateTime parsed)
        {
            // Check if the input ends with UTC indicators
            // Note: We check the trimmed input to handle whitespace
            string trimmedInput = input.TrimEnd();

            if (trimmedInput.EndsWith("+00") ||
                trimmedInput.EndsWith("-00") ||
                trimmedInput.EndsWith("+00:00") ||
                trimmedInput.EndsWith("-00:00"))
            {
                // These indicate UTC (offset of zero from UTC)

                if (parsed.Kind == DateTimeKind.Utc)
                {
                    // Already UTC, no change needed
                    return parsed;
                }
                else if (parsed.Kind == DateTimeKind.Local)
                {
                    // The "zzz" format specifier converted to local time
                    // We need to convert back to UTC since the input was actually UTC
                    return parsed.ToUniversalTime();
                }
                else // Unspecified
                {
                    // Literal format match (like "+00") - just set the kind
                    return DateTime.SpecifyKind(parsed, DateTimeKind.Utc);
                }
            }

            // Return unchanged
            return parsed;
        }

#pragma warning restore CS8625 // Cannot convert null literal to non-nullable reference type.
    }
}
