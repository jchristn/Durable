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
}