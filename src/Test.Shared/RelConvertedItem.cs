namespace Test.Shared
{
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// Entity whose properties are mapped through <see cref="ValueConverterAttribute"/> converters.
    /// </summary>
    [Entity("rel_converted_items")]
    public class RelConvertedItem
    {
        /// <summary>
        /// Gets or sets the identifier.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the name. Never null.
        /// </summary>
        [Property("name", Flags.String, 100)]
        public string Name { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the price, stored as cents.
        /// </summary>
        [Property("price")]
        [ValueConverter(typeof(MoneyConverter))]
        public Money Price { get; set; }

        /// <summary>
        /// Gets or sets the tags, stored as a comma-separated string. Never null.
        /// </summary>
        [Property("tags", Flags.String, 500)]
        [ValueConverter(typeof(CsvListConverter))]
        public List<string> Tags { get; set; } = new List<string>();

        /// <summary>
        /// Gets or sets the priority, stored as a one-letter code.
        /// </summary>
        [Property("priority", Flags.String, 1)]
        [ValueConverter(typeof(PriorityCodeConverter))]
        public RelPriority Priority { get; set; }
    }
}
