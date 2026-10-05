namespace Test.Shared
{
    using System;
    using Durable;

    /// <summary>
    /// Migration test entity covering every common column type, a single-column index and a composite unique index.
    /// </summary>
    [Entity("mig_all_types")]
    [CompositeIndex("idx_mig_all_code_flag", "code", "flag", IsUnique = true)]
    public class MigAllTypes
    {
        /// <summary>
        /// Gets or sets the identifier.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the name (indexed, max 50). Never null.
        /// </summary>
        [Property("name", Flags.String, 50)]
        [Index]
        public string Name { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets an optional code (max 20).
        /// </summary>
        [Property("code", Flags.String, 20)]
        public string? Code { get; set; }

        /// <summary>
        /// Gets or sets unbounded notes.
        /// </summary>
        [Property("notes")]
        public string? Notes { get; set; }

        /// <summary>
        /// Gets or sets a 32-bit integer.
        /// </summary>
        [Property("count")]
        public int Count { get; set; }

        /// <summary>
        /// Gets or sets a 64-bit integer.
        /// </summary>
        [Property("big")]
        public long Big { get; set; }

        /// <summary>
        /// Gets or sets a boolean.
        /// </summary>
        [Property("flag")]
        public bool Flag { get; set; }

        /// <summary>
        /// Gets or sets a decimal.
        /// </summary>
        [Property("amount")]
        public decimal Amount { get; set; }

        /// <summary>
        /// Gets or sets a double.
        /// </summary>
        [Property("ratio")]
        public double Ratio { get; set; }

        /// <summary>
        /// Gets or sets a date/time.
        /// </summary>
        [Property("created")]
        public DateTime Created { get; set; }

        /// <summary>
        /// Gets or sets an optional date/time with offset.
        /// </summary>
        [Property("stamp")]
        public DateTimeOffset? Stamp { get; set; }

        /// <summary>
        /// Gets or sets a GUID.
        /// </summary>
        [Property("uid")]
        public Guid Uid { get; set; }

        /// <summary>
        /// Gets or sets optional binary data.
        /// </summary>
        [Property("data")]
        public byte[]? Data { get; set; }

        /// <summary>
        /// Gets or sets an enum stored as a string.
        /// </summary>
        [Property("status")]
        public Status Status { get; set; }

        /// <summary>
        /// Gets or sets an enum stored as an integer.
        /// </summary>
        [Property("priority", Flags.Integer)]
        public Status Priority { get; set; }

        /// <summary>
        /// Gets or sets an optional integer.
        /// </summary>
        [Property("maybe")]
        public int? Maybe { get; set; }
    }
}
