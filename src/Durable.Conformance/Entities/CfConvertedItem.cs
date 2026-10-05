namespace Durable.Conformance
{
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// Entity whose properties are stored through value converters (struct to integer, list to text, enum to code).
    /// Storage: <c>cf_converted_items</c>.
    /// </summary>
    [Entity("cf_converted_items")]
    public class CfConvertedItem
    {
        /// <summary>Gets or sets the generated identity key.</summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>Gets or sets the name. Never null.</summary>
        [Property("name", Flags.String, 64)]
        public string Name { get; set; } = string.Empty;

        /// <summary>Gets or sets the price (stored as cents).</summary>
        [Property("price")]
        [ValueConverter(typeof(CfMoneyConverter))]
        public CfMoney Price { get; set; }

        /// <summary>Gets or sets the tags (stored as comma-separated text). Never null.</summary>
        [Property("tags", Flags.String, 256)]
        [ValueConverter(typeof(CfCsvListConverter))]
        public List<string> Tags { get; set; } = new List<string>();

        /// <summary>Gets or sets the grade (stored as a one-letter code).</summary>
        [Property("grade", Flags.String, 1)]
        [ValueConverter(typeof(CfGradeCodeConverter))]
        public CfGrade Grade { get; set; }
    }
}
