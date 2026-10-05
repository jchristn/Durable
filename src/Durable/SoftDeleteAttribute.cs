namespace Durable
{
    using System;

    /// <summary>
    /// Marks a column as the soft-delete marker for its entity.
    /// The property must be a <see cref="bool"/> (true means deleted) or a nullable
    /// <see cref="DateTime"/> / <see cref="DateTimeOffset"/> (non-null means deleted).
    /// When present, delete operations set the marker instead of removing rows, and queries exclude
    /// deleted rows unless <see cref="IQueryBuilder{T}.IgnoreQueryFilters"/> is used.
    /// At most one property per entity may carry this attribute.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class SoftDeleteAttribute : Attribute
    {
    }
}
