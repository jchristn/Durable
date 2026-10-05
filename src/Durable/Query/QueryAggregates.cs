namespace Durable.Query
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;

    /// <summary>
    /// Computes Sum, Average, Min and Max over values with SQL aggregate semantics, for backends and query builders that
    /// aggregate on the client: null values are ignored, and an aggregate over no non-null values is null.
    /// Sums of integral and decimal values are exact (decimal); sums involving double or float are computed in double.
    /// Averages are computed in decimal, like Durable's SQL engine (<c>AVG(CAST(x AS DECIMAL(38, 10)))</c>).
    /// Thread safety: stateless; safe for concurrent use.
    /// </summary>
    public static class QueryAggregates
    {
        #region Public-Methods

        /// <summary>
        /// Sums values, ignoring nulls.
        /// </summary>
        /// <param name="values">Values. Must not be null.</param>
        /// <returns>The sum as decimal or double, or null when there are no non-null values.</returns>
        /// <exception cref="ArgumentNullException">Thrown when values is null.</exception>
        public static object? Sum(IEnumerable<object?> values)
        {
            ArgumentNullException.ThrowIfNull(values);
            bool any = false;
            bool floating = false;
            decimal exact = 0m;
            double approximate = 0d;
            foreach (object? value in values)
            {
                if (value == null) continue;
                any = true;
                if (!floating && (value is double || value is float))
                {
                    floating = true;
                    approximate = (double)exact;
                }

                if (floating) approximate += ToDouble(value);
                else exact += ToDecimal(value);
            }

            if (!any) return null;
            return floating ? approximate : exact;
        }

        /// <summary>
        /// Averages values in decimal, ignoring nulls.
        /// </summary>
        /// <param name="values">Values. Must not be null.</param>
        /// <returns>The average, or null when there are no non-null values.</returns>
        /// <exception cref="ArgumentNullException">Thrown when values is null.</exception>
        public static decimal? Average(IEnumerable<object?> values)
        {
            ArgumentNullException.ThrowIfNull(values);
            decimal total = 0m;
            long count = 0;
            foreach (object? value in values)
            {
                if (value == null) continue;
                total += ToDecimal(value);
                count++;
            }

            return count == 0 ? null : total / count;
        }

        /// <summary>
        /// Returns the smallest value, ignoring nulls.
        /// </summary>
        /// <param name="values">Values. Must not be null.</param>
        /// <param name="comparer">Comparer; null uses <see cref="QueryValueComparer.Ordinal"/>.</param>
        /// <returns>The minimum, or null when there are no non-null values.</returns>
        /// <exception cref="ArgumentNullException">Thrown when values is null.</exception>
        public static object? Min(IEnumerable<object?> values, IComparer<object?>? comparer = null)
        {
            return Extreme(values, comparer, -1);
        }

        /// <summary>
        /// Returns the largest value, ignoring nulls.
        /// </summary>
        /// <param name="values">Values. Must not be null.</param>
        /// <param name="comparer">Comparer; null uses <see cref="QueryValueComparer.Ordinal"/>.</param>
        /// <returns>The maximum, or null when there are no non-null values.</returns>
        /// <exception cref="ArgumentNullException">Thrown when values is null.</exception>
        public static object? Max(IEnumerable<object?> values, IComparer<object?>? comparer = null)
        {
            return Extreme(values, comparer, 1);
        }

        /// <summary>
        /// Computes an aggregate by function.
        /// </summary>
        /// <param name="function"><see cref="AggregateFunction.Sum"/>, <see cref="AggregateFunction.Average"/>, <see cref="AggregateFunction.Min"/> or <see cref="AggregateFunction.Max"/>.</param>
        /// <param name="values">Values. Must not be null.</param>
        /// <param name="comparer">Comparer for Min and Max; null uses <see cref="QueryValueComparer.Ordinal"/>.</param>
        /// <returns>The aggregate, or null when there are no non-null values.</returns>
        /// <exception cref="ArgumentNullException">Thrown when values is null.</exception>
        /// <exception cref="NotSupportedException">Thrown for Count and Any.</exception>
        public static object? Compute(AggregateFunction function, IEnumerable<object?> values, IComparer<object?>? comparer = null)
        {
            switch (function)
            {
                case AggregateFunction.Sum: return Sum(values);
                case AggregateFunction.Average: return Average(values);
                case AggregateFunction.Min: return Min(values, comparer);
                case AggregateFunction.Max: return Max(values, comparer);
                default: throw new NotSupportedException("Aggregate " + function + " is not a value aggregate.");
            }
        }

        #endregion

        #region Private-Methods

        private static object? Extreme(IEnumerable<object?> values, IComparer<object?>? comparer, int direction)
        {
            ArgumentNullException.ThrowIfNull(values);
            IComparer<object?> effective = comparer ?? QueryValueComparer.Ordinal;
            object? best = null;
            foreach (object? value in values)
            {
                if (value == null) continue;
                if (best == null || effective.Compare(value, best) * direction > 0) best = value;
            }

            return best;
        }

        private static decimal ToDecimal(object value)
        {
            if (value is Enum) return Convert.ToInt64(value, CultureInfo.InvariantCulture);
            if (value is bool b) return b ? 1m : 0m;
            return Convert.ToDecimal(value, CultureInfo.InvariantCulture);
        }

        private static double ToDouble(object value)
        {
            if (value is Enum) return Convert.ToInt64(value, CultureInfo.InvariantCulture);
            if (value is bool b) return b ? 1d : 0d;
            return Convert.ToDouble(value, CultureInfo.InvariantCulture);
        }

        #endregion
    }
}
