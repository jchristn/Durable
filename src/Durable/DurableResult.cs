namespace Durable
{
    using System.Collections.Generic;

    public class DurableResult<T> : IDurableResult<T>
    {
        public string Query { get; }
        public IEnumerable<T> Result { get; }

        public DurableResult(string query, IEnumerable<T> result)
        {
            Query = query;
            Result = result;
        }
    }
    
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