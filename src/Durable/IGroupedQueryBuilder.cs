namespace Durable
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Text;
    using Microsoft.Data.Sqlite;

    public interface IGroupedQueryBuilder<T, TKey> where T : class, new()
    {
        IGroupedQueryBuilder<T, TKey> Having(Expression<Func<IGrouping<TKey, T>, bool>> predicate);
        IEnumerable<IGrouping<TKey, T>> Execute();
        Task<IEnumerable<IGrouping<TKey, T>>> ExecuteAsync(CancellationToken token = default);
    }
}