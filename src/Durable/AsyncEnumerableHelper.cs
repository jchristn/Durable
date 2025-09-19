namespace Durable
{
    using System.Collections.Generic;
    using System.Threading.Tasks;

    /// <summary>
    /// Helper class to provide empty async enumerables.
    /// </summary>
    internal static class AsyncEnumerableHelper
    {
        /// <summary>
        /// Returns an empty async enumerable of the specified type.
        /// </summary>
        /// <typeparam name="T">The type of elements in the async enumerable.</typeparam>
        /// <returns>An empty async enumerable.</returns>
        public static async IAsyncEnumerable<T> Empty<T>()
        {
            await Task.CompletedTask;
            yield break;
        }
    }
}