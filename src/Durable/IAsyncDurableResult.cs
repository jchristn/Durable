namespace Durable
{
    using System.Collections.Generic;

    public interface IAsyncDurableResult<T>
    {
        string Query { get; }
        IAsyncEnumerable<T> Result { get; }
    }
}