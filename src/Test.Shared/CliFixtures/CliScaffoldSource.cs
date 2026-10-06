namespace Test.Shared.CliFixtures.Scaffold
{
    using System;
    using Durable;

    /// <summary>
    /// CLI fixture entity whose table is created with schema sync and then reverse-engineered with 'durable scaffold';
    /// the scaffolded class must map back to the same schema.
    /// </summary>
    [Entity("cli_scaffold_items")]
    [CompositeIndex("idx_cli_scaffold_code_count", "code", "item_count", IsUnique = true)]
    public class CliScaffoldSource
    {
        /// <summary>Gets or sets the key.</summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>Gets or sets the name.</summary>
        [Property("name", Flags.String, 80)]
        [Index("idx_cli_scaffold_name")]
        public string Name { get; set; } = string.Empty;

        /// <summary>Gets or sets the code.</summary>
        [Property("code", Flags.String, 20)]
        public string Code { get; set; } = string.Empty;

        /// <summary>Gets or sets the notes.</summary>
        [Property("notes")]
        public string? Notes { get; set; }

        /// <summary>Gets or sets the count.</summary>
        [Property("item_count")]
        public int ItemCount { get; set; }

        /// <summary>Gets or sets a large number.</summary>
        [Property("big")]
        public long Big { get; set; }

        /// <summary>Gets or sets a flag.</summary>
        [Property("flag")]
        public bool Flag { get; set; }

        /// <summary>Gets or sets an amount.</summary>
        [Property("amount")]
        public decimal Amount { get; set; }

        /// <summary>Gets or sets a ratio.</summary>
        [Property("ratio")]
        public double Ratio { get; set; }

        /// <summary>Gets or sets a timestamp.</summary>
        [Property("created")]
        public DateTime Created { get; set; }

        /// <summary>Gets or sets a unique identifier.</summary>
        [Property("uid")]
        public Guid Uid { get; set; }

        /// <summary>Gets or sets binary data.</summary>
        [Property("data")]
        public byte[]? Data { get; set; }

        /// <summary>Gets or sets an optional number.</summary>
        [Property("maybe")]
        public int? Maybe { get; set; }
    }
}
