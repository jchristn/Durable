namespace Test.Shared
{
    using Durable;

    /// <summary>
    /// Entity used to verify that generated keys are assigned in input order across insert chunks.
    /// </summary>
    [Entity("rel_sequence_items")]
    public class RelSequenceItem
    {
        /// <summary>
        /// Gets or sets the identifier.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the input position.
        /// </summary>
        [Property("ordinal")]
        public int Ordinal { get; set; }

        /// <summary>
        /// Gets or sets the label. Never null.
        /// </summary>
        [Property("label", Flags.String, 50)]
        public string Label { get; set; } = string.Empty;
    }
}
