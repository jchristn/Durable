namespace Test.Shared
{
    using System;
    using Durable;

    /// <summary>
    /// Test entity representing a category.
    /// Used by <see cref="InitializationTests"/>.
    /// </summary>
    [Entity("test_categories")]
    public class InitCategory
    {
        /// <summary>
        /// Gets or sets the category ID.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the category name.
        /// </summary>
        [Property("name")]
        public string Name { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the category GUID.
        /// </summary>
        [Property("guid")]
        [DefaultValue(DefaultValueType.SequentialGuid)]
        public Guid CategoryGuid { get; set; }
    }
}
