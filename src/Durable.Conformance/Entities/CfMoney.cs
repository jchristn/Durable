namespace Durable.Conformance
{
    using System;

    /// <summary>
    /// Money amount in cents; stored as a 64-bit integer through <see cref="CfMoneyConverter"/>.
    /// Thread safety: immutable.
    /// </summary>
    public readonly struct CfMoney : IEquatable<CfMoney>
    {
        /// <summary>
        /// Gets the amount in cents.
        /// </summary>
        public long Cents { get; }

        /// <summary>
        /// Initializes an amount.
        /// </summary>
        /// <param name="cents">Amount in cents.</param>
        public CfMoney(long cents)
        {
            Cents = cents;
        }

        /// <summary>
        /// Compares two amounts.
        /// </summary>
        /// <param name="left">Left amount.</param>
        /// <param name="right">Right amount.</param>
        /// <returns>True when equal.</returns>
        public static bool operator ==(CfMoney left, CfMoney right)
        {
            return left.Equals(right);
        }

        /// <summary>
        /// Compares two amounts.
        /// </summary>
        /// <param name="left">Left amount.</param>
        /// <param name="right">Right amount.</param>
        /// <returns>True when different.</returns>
        public static bool operator !=(CfMoney left, CfMoney right)
        {
            return !left.Equals(right);
        }

        /// <summary>
        /// Compares with another amount.
        /// </summary>
        /// <param name="other">Other amount.</param>
        /// <returns>True when equal.</returns>
        public bool Equals(CfMoney other)
        {
            return Cents == other.Cents;
        }

        /// <summary>
        /// Compares with another object.
        /// </summary>
        /// <param name="obj">Other object; may be null.</param>
        /// <returns>True when obj is an equal amount.</returns>
        public override bool Equals(object? obj)
        {
            return obj is CfMoney other && Equals(other);
        }

        /// <summary>
        /// Gets a hash code.
        /// </summary>
        /// <returns>The hash code.</returns>
        public override int GetHashCode()
        {
            return Cents.GetHashCode();
        }

        /// <summary>
        /// Formats the amount.
        /// </summary>
        /// <returns>For example "1999c".</returns>
        public override string ToString()
        {
            return Cents + "c";
        }
    }
}
