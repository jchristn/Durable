namespace Test.Shared
{
    using System;

    /// <summary>
    /// A custom key type stored as text through <see cref="RegCodeConverter"/>.
    /// </summary>
    public readonly struct RegCode : IEquatable<RegCode>
    {
        /// <summary>
        /// Gets the code text. Never null for a constructed value.
        /// </summary>
        public string Value { get; }

        /// <summary>
        /// Instantiates the code.
        /// </summary>
        /// <param name="value">The code text. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when value is null.</exception>
        public RegCode(string value)
        {
            Value = value ?? throw new ArgumentNullException(nameof(value));
        }

        /// <inheritdoc />
        public bool Equals(RegCode other)
        {
            return string.Equals(Value, other.Value, StringComparison.Ordinal);
        }

        /// <inheritdoc />
        public override bool Equals(object? obj)
        {
            return obj is RegCode other && Equals(other);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return Value == null ? 0 : StringComparer.Ordinal.GetHashCode(Value);
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return Value ?? string.Empty;
        }

        /// <summary>
        /// Compares two codes for equality.
        /// </summary>
        /// <param name="left">The left code.</param>
        /// <param name="right">The right code.</param>
        /// <returns>True when the codes are equal.</returns>
        public static bool operator ==(RegCode left, RegCode right)
        {
            return left.Equals(right);
        }

        /// <summary>
        /// Compares two codes for inequality.
        /// </summary>
        /// <param name="left">The left code.</param>
        /// <param name="right">The right code.</param>
        /// <returns>True when the codes differ.</returns>
        public static bool operator !=(RegCode left, RegCode right)
        {
            return !left.Equals(right);
        }
    }
}
