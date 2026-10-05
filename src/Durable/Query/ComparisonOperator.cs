namespace Durable.Query
{
    /// <summary>
    /// Binary comparison operators in a <see cref="ComparisonNode"/>.
    /// </summary>
    public enum ComparisonOperator
    {
        /// <summary>Equal (C# semantics: null equals null).</summary>
        Equal,
        /// <summary>Not equal (C# semantics: null differs from any value).</summary>
        NotEqual,
        /// <summary>Less than.</summary>
        LessThan,
        /// <summary>Less than or equal.</summary>
        LessThanOrEqual,
        /// <summary>Greater than.</summary>
        GreaterThan,
        /// <summary>Greater than or equal.</summary>
        GreaterThanOrEqual
    }
}
