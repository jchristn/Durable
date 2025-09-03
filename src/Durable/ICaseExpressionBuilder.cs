namespace Durable
{
    using System;
    using System.Linq.Expressions;

    public interface ICaseExpressionBuilder<T> where T : class, new()
    {
        ICaseExpressionBuilder<T> When(Expression<Func<T, bool>> condition, object result);
        ICaseExpressionBuilder<T> WhenRaw(string condition, object result);
        ICaseExpressionBuilder<T> Else(object result);
        IQueryBuilder<T> EndCase(string alias);
    }
}