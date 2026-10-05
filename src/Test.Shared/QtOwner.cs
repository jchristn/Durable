namespace Test.Shared
{
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// Owner entity for the query translation suites (table <c>qt_owners</c>). Owns zero or more <see cref="QtItem"/> rows.
    /// The table is created in-suite through <c>InitializeTable</c>.
    /// </summary>
    [Entity("qt_owners")]
    public class QtOwner
    {
        /// <summary>
        /// Gets or sets the primary key.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the owner name. Never null.
        /// </summary>
        [Property("name", Flags.String, 64)]
        public string Name { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the city. May be null.
        /// </summary>
        [Property("city", Flags.String, 64)]
        public string? City { get; set; }

        /// <summary>
        /// Gets or sets the owned items (inverse of <see cref="QtItem.OwnerId"/>). Never null.
        /// </summary>
        [InverseNavigationProperty("OwnerId")]
        public List<QtItem> Items { get; set; } = new List<QtItem>();
    }
}
