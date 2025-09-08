namespace Durable
{
    using System.Collections.Generic;

    public class AsyncDurableResult<T> : IAsyncDurableResult<T>
    {
        public string Query { get; }
        public IAsyncEnumerable<T> Result { get; }

        public AsyncDurableResult(string query, IAsyncEnumerable<T> result)
        {
            Query = query;
            Result = result;
        }
    }
}