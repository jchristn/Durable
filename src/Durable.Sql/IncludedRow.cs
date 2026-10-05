namespace Durable.Sql
{
    /// <summary>
    /// A related entity loaded by <see cref="IncludeLoader"/> together with the normalized key of the owner it belongs to.
    /// </summary>
    internal sealed class IncludedRow
    {
        internal IncludedRow(object entity, object? ownerKey)
        {
            Entity = entity;
            OwnerKey = ownerKey;
        }

        internal object Entity { get; }

        internal object? OwnerKey { get; }
    }
}
