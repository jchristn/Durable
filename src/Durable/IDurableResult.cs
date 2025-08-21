namespace Durable
{
    using System.Collections.Generic;

    public interface IDurableResult<T>
    {
        string Query { get; }
        IEnumerable<T> Result { get; }
    }
    
    public interface IAsyncDurableResult<T>
    {
        string Query { get; }
        IAsyncEnumerable<T> Result { get; }
    }
}