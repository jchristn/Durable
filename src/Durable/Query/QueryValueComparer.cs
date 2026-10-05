namespace Durable.Query
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using Durable;

    /// <summary>
    /// Compares query values the way a database compares column values, for backends that evaluate queries on the client.
    /// Numbers of different CLR types compare by value (int 1 equals long 1 and decimal 1.0); enums compare with numbers by
    /// their underlying value and with strings by name; chars compare with strings as one-character strings; byte arrays
    /// compare by content; <see cref="DateTime"/> and <see cref="DateTimeOffset"/> compare by UTC instant. Strings compare
    /// by <see cref="Mode"/>: <see cref="StringMatchMode.Ordinal"/> and <see cref="StringMatchMode.Database"/> are ordinal,
    /// <see cref="StringMatchMode.IgnoreCase"/> is <see cref="StringComparison.OrdinalIgnoreCase"/>.
    /// Null sorts before every value and equals only null.
    /// Thread safety: immutable; safe for concurrent use.
    /// </summary>
    public sealed class QueryValueComparer : IComparer<object?>, IEqualityComparer<object?>
    {
        #region Public-Members

        /// <summary>
        /// Gets the comparer using ordinal string comparison. Never null.
        /// </summary>
        public static QueryValueComparer Ordinal { get; } = new QueryValueComparer(StringMatchMode.Ordinal);

        /// <summary>
        /// Gets the comparer using ordinal ignore-case string comparison. Never null.
        /// </summary>
        public static QueryValueComparer IgnoreCase { get; } = new QueryValueComparer(StringMatchMode.IgnoreCase);

        /// <summary>
        /// Gets the string comparison mode.
        /// </summary>
        public StringMatchMode Mode { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a comparer.
        /// </summary>
        /// <param name="mode">String comparison mode; <see cref="StringMatchMode.Database"/> is treated as ordinal.</param>
        public QueryValueComparer(StringMatchMode mode)
        {
            Mode = mode;
        }

        /// <summary>
        /// Returns the shared comparer for a mode.
        /// </summary>
        /// <param name="mode">String comparison mode.</param>
        /// <returns>The comparer. Never null.</returns>
        public static QueryValueComparer For(StringMatchMode mode)
        {
            return mode == StringMatchMode.IgnoreCase ? IgnoreCase : Ordinal;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Gets the <see cref="StringComparison"/> used for strings under a mode.
        /// </summary>
        /// <param name="mode">Mode.</param>
        /// <returns><see cref="StringComparison.OrdinalIgnoreCase"/> for <see cref="StringMatchMode.IgnoreCase"/>, otherwise <see cref="StringComparison.Ordinal"/>.</returns>
        public static StringComparison StringComparisonFor(StringMatchMode mode)
        {
            return mode == StringMatchMode.IgnoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
        }

        /// <inheritdoc />
        /// <exception cref="ArgumentException">Thrown when the values cannot be compared.</exception>
        public int Compare(object? x, object? y)
        {
            if (x == null) return y == null ? 0 : -1;
            if (y == null) return 1;

            Normalize(ref x, ref y);
            return CompareNonNull(x!, y!);
        }

        /// <inheritdoc />
        public new bool Equals(object? x, object? y)
        {
            if (x == null || y == null) return x == null && y == null;
            Normalize(ref x, ref y);
            return EqualsNonNull(x!, y!);
        }

        /// <inheritdoc />
        public int GetHashCode(object? obj)
        {
            if (obj == null) return 0;
            object? value = obj;
            object? other = obj;
            Normalize(ref value, ref other);
            return HashNonNull(value!);
        }

        #endregion

        #region Private-Methods

        private int CompareNonNull(object x, object y)
        {
            if (x is string sx && y is string sy) return Math.Sign(string.Compare(sx, sy, StringComparisonFor(Mode)));
            if (IsNumber(x) && IsNumber(y)) return CompareNumbers(x, y);
            if (x is byte[] bx && y is byte[] by) return CompareBytes(bx, by);
            if (x is DateTimeOffset dx && y is DateTimeOffset dy) return dx.UtcDateTime.CompareTo(dy.UtcDateTime);
            if (x.GetType() == y.GetType() && x is IComparable comparable) return Math.Sign(comparable.CompareTo(y));
            if (x is IComparable fallback)
            {
                try
                {
                    return Math.Sign(fallback.CompareTo(y));
                }
                catch (ArgumentException)
                {
                }
            }

            throw new ArgumentException("Cannot compare " + x.GetType().Name + " with " + y.GetType().Name + ".");
        }

        private bool EqualsNonNull(object x, object y)
        {
            if (x is string sx && y is string sy) return string.Equals(sx, sy, StringComparisonFor(Mode));
            if (IsNumber(x) && IsNumber(y)) return CompareNumbers(x, y) == 0;
            if (x is byte[] bx && y is byte[] by) return CompareBytes(bx, by) == 0;
            if (x is DateTimeOffset dx && y is DateTimeOffset dy) return dx.UtcDateTime == dy.UtcDateTime;
            if (x.Equals(y)) return true;
            if (x.GetType() != y.GetType() && x is IComparable comparable)
            {
                try
                {
                    return comparable.CompareTo(y) == 0;
                }
                catch (ArgumentException)
                {
                    return false;
                }
            }

            return false;
        }

        private int HashNonNull(object value)
        {
            switch (value)
            {
                case string s:
                    return Mode == StringMatchMode.IgnoreCase ? StringComparer.OrdinalIgnoreCase.GetHashCode(s) : StringComparer.Ordinal.GetHashCode(s);
                case byte[] bytes:
                    {
                        int hash = bytes.Length;
                        foreach (byte b in bytes) hash = unchecked(hash * 31 + b);
                        return hash;
                    }
                case DateTimeOffset offset:
                    return offset.UtcDateTime.GetHashCode();
            }

            if (IsNumber(value))
            {
                decimal? asDecimal = ToDecimalOrNull(value);
                return asDecimal.HasValue ? asDecimal.Value.GetHashCode() : Convert.ToDouble(value, CultureInfo.InvariantCulture).GetHashCode();
            }

            return value.GetHashCode();
        }

        private static void Normalize(ref object? x, ref object? y)
        {
            if (x is Enum ex)
            {
                if (y is string) x = ex.ToString();
                else if (y is not Enum || y.GetType() != x.GetType()) x = Convert.ToInt64(ex, CultureInfo.InvariantCulture);
            }

            if (y is Enum ey)
            {
                if (x is string) y = ey.ToString();
                else if (x is not Enum || x.GetType() != y.GetType()) y = Convert.ToInt64(ey, CultureInfo.InvariantCulture);
            }

            if (x is char cx && y is string) x = cx.ToString();
            if (y is char cy && x is string) y = cy.ToString();
            if (x is DateTime dtx && y is DateTimeOffset) x = new DateTimeOffset(DateTime.SpecifyKind(dtx, dtx.Kind == DateTimeKind.Unspecified ? DateTimeKind.Utc : dtx.Kind));
            if (y is DateTime dty && x is DateTimeOffset) y = new DateTimeOffset(DateTime.SpecifyKind(dty, dty.Kind == DateTimeKind.Unspecified ? DateTimeKind.Utc : dty.Kind));
        }

        private static bool IsNumber(object value)
        {
            return value is int || value is long || value is short || value is byte || value is sbyte || value is uint || value is ulong || value is ushort
                || value is decimal || value is double || value is float;
        }

        private static int CompareNumbers(object x, object y)
        {
            if (x is double || x is float || y is double || y is float)
                return Convert.ToDouble(x, CultureInfo.InvariantCulture).CompareTo(Convert.ToDouble(y, CultureInfo.InvariantCulture));
            if (x is decimal || y is decimal)
                return Convert.ToDecimal(x, CultureInfo.InvariantCulture).CompareTo(Convert.ToDecimal(y, CultureInfo.InvariantCulture));
            if (x is ulong ux && y is ulong uy) return ux.CompareTo(uy);
            if (x is ulong || y is ulong)
                return Convert.ToDecimal(x, CultureInfo.InvariantCulture).CompareTo(Convert.ToDecimal(y, CultureInfo.InvariantCulture));
            return Convert.ToInt64(x, CultureInfo.InvariantCulture).CompareTo(Convert.ToInt64(y, CultureInfo.InvariantCulture));
        }

        private static decimal? ToDecimalOrNull(object value)
        {
            if (value is double d)
            {
                if (double.IsNaN(d) || double.IsInfinity(d) || Math.Abs(d) > 7.9e28) return null;
                return (decimal)d;
            }

            if (value is float f)
            {
                if (float.IsNaN(f) || float.IsInfinity(f) || Math.Abs(f) > 7.9e28f) return null;
                return (decimal)f;
            }

            return Convert.ToDecimal(value, CultureInfo.InvariantCulture);
        }

        private static int CompareBytes(byte[] x, byte[] y)
        {
            int length = Math.Min(x.Length, y.Length);
            for (int i = 0; i < length; i++)
            {
                if (x[i] != y[i]) return x[i] < y[i] ? -1 : 1;
            }

            return x.Length.CompareTo(y.Length);
        }

        #endregion
    }
}
