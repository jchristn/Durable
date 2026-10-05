namespace Durable.Query
{
    /// <summary>
    /// Scalar functions recognized by <see cref="QueryNormalizer"/> in LINQ expressions; each backend translates them (SQL dialects via <c>ISqlDialect.TranslateFunction</c>).
    /// </summary>
    public enum QueryFunction
    {
        /// <summary>String length in characters. Arguments: string.</summary>
        Length,
        /// <summary>Upper-case. Arguments: string.</summary>
        Upper,
        /// <summary>Lower-case. Arguments: string.</summary>
        Lower,
        /// <summary>Trim both ends. Arguments: string.</summary>
        Trim,
        /// <summary>Trim start. Arguments: string.</summary>
        TrimStart,
        /// <summary>Trim end. Arguments: string.</summary>
        TrimEnd,
        /// <summary>Substring with a zero-based start. Arguments: string, start, [length].</summary>
        Substring,
        /// <summary>Replace. Arguments: string, old, new.</summary>
        Replace,
        /// <summary>Zero-based index of a substring, or -1. Arguments: string, value.</summary>
        IndexOf,
        /// <summary>Absolute value. Arguments: number.</summary>
        Abs,
        /// <summary>Round. Arguments: number, [digits].</summary>
        Round,
        /// <summary>Ceiling. Arguments: number.</summary>
        Ceiling,
        /// <summary>Floor. Arguments: number.</summary>
        Floor,
        /// <summary>Power. Arguments: base, exponent.</summary>
        Power,
        /// <summary>Square root. Arguments: number.</summary>
        Sqrt,
        /// <summary>Year part. Arguments: date.</summary>
        Year,
        /// <summary>Month part. Arguments: date.</summary>
        Month,
        /// <summary>Day-of-month part. Arguments: date.</summary>
        Day,
        /// <summary>Hour part. Arguments: date.</summary>
        Hour,
        /// <summary>Minute part. Arguments: date.</summary>
        Minute,
        /// <summary>Second part. Arguments: date.</summary>
        Second,
        /// <summary>Day of year (1-366). Arguments: date.</summary>
        DayOfYear,
        /// <summary>Day of week (0 = Sunday, matching <see cref="System.DayOfWeek"/>). Arguments: date.</summary>
        DayOfWeek,
        /// <summary>Date part with time removed. Arguments: date.</summary>
        Date,
        /// <summary>Add years. Arguments: date, amount.</summary>
        AddYears,
        /// <summary>Add months. Arguments: date, amount.</summary>
        AddMonths,
        /// <summary>Add days. Arguments: date, amount.</summary>
        AddDays,
        /// <summary>Add hours. Arguments: date, amount.</summary>
        AddHours,
        /// <summary>Add minutes. Arguments: date, amount.</summary>
        AddMinutes,
        /// <summary>Add seconds. Arguments: date, amount.</summary>
        AddSeconds
    }
}
