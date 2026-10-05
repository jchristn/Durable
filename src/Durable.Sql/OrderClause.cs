namespace Durable.Sql
{
    using System.Linq.Expressions;

    /// <summary>
    /// An ORDER BY key selector and direction. Internal to the query builders.
    /// </summary>
    internal sealed class OrderClause
    {
        internal OrderClause(LambdaExpression keySelector, bool descending)
        {
            KeySelector = keySelector;
            Descending = descending;
        }

        internal LambdaExpression KeySelector { get; }

        internal bool Descending { get; }
    }
}
