namespace Durable
{
    using System;

    /// <summary>
    /// Marks a property as the optimistic-concurrency version column: updates only succeed when the stored value still
    /// equals the value the caller read, and every write replaces it (see <see cref="VersionColumnType"/>). At most one
    /// property per entity may carry this attribute. Thread safety: immutable.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class VersionColumnAttribute : Attribute
    {
        /// <summary>
        /// Gets the declared version type, or null when it is inferred from the property type: integer types use
        /// <see cref="VersionColumnType.Integer"/>, <see cref="DateTime"/> uses <see cref="VersionColumnType.Timestamp"/>,
        /// <see cref="System.Guid"/> uses <see cref="VersionColumnType.Guid"/> and <c>byte[]</c> uses
        /// <see cref="VersionColumnType.BinaryCounter"/>.
        /// </summary>
        public VersionColumnType? Type { get; }

        /// <summary>
        /// Marks a version column whose type is inferred from the property type (see <see cref="Type"/>).
        /// </summary>
        public VersionColumnAttribute()
        {
        }

        /// <summary>
        /// Marks a version column of the given type. The property type must match it (validated when the entity's
        /// metadata is built).
        /// </summary>
        /// <param name="type">The version type.</param>
        public VersionColumnAttribute(VersionColumnType type)
        {
            Type = type;
        }
    }
}
