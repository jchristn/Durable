namespace Durable.Query
{
    /// <summary>
    /// Arithmetic operators in an <see cref="ArithmeticNode"/>.
    /// </summary>
    public enum ArithmeticOperator
    {
        /// <summary>Addition.</summary>
        Add,
        /// <summary>Subtraction.</summary>
        Subtract,
        /// <summary>Multiplication.</summary>
        Multiply,
        /// <summary>Division (see <see cref="ArithmeticNode.IntegerDivision"/>).</summary>
        Divide,
        /// <summary>Remainder.</summary>
        Modulo
    }
}
