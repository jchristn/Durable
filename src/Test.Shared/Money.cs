namespace Test.Shared
{
    using System;

    /// <summary>
    /// Simple money value stored as whole cents. Used to exercise <c>ValueConverter</c> mapping.
    /// Thread safety: immutable.
    /// </summary>
    public readonly struct Money : IEquatable<Money>
    {
        /// <summary>
        /// Gets the amount in cents.
        /// </summary>
        public long Cents { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="Money"/> struct.
        /// </summary>
        /// <param name="cents">Amount in cents.</param>
        public Money(long cents)
        {
            Cents = cents;
        }

        /// <summary>
        /// Equality operator.
        /// </summary>
        /// <param name="left">Left operand.</param>
        /// <param name="right">Right operand.</param>
        /// <returns>True when equal.</returns>
        public static bool operator ==(Money left, Money right)
        {
            return left.Equals(right);
        }

        /// <summary>
        /// Inequality operator.
        /// </summary>
        /// <param name="left">Left operand.</param>
        /// <param name="right">Right operand.</param>
        /// <returns>True when not equal.</returns>
        public static bool operator !=(Money left, Money right)
        {
            return !left.Equals(right);
        }

        /// <summary>
        /// Compares with another money value.
        /// </summary>
        /// <param name="other">Other value.</param>
        /// <returns>True when equal.</returns>
        public bool Equals(Money other)
        {
            return Cents == other.Cents;
        }

        /// <summary>
        /// Compares with another object.
        /// </summary>
        /// <param name="obj">Other object; may be null.</param>
        /// <returns>True when equal.</returns>
        public override bool Equals(object? obj)
        {
            return obj is Money other && Equals(other);
        }

        /// <summary>
        /// Gets the hash code.
        /// </summary>
        /// <returns>Hash code.</returns>
        public override int GetHashCode()
        {
            return Cents.GetHashCode();
        }

        /// <summary>
        /// Formats the value.
        /// </summary>
        /// <returns>The value in cents.</returns>
        public override string ToString()
        {
            return Cents + "c";
        }
    }
}
