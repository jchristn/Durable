namespace Test.Shared
{
    using Durable;

    /// <summary>
    /// Entity with a binary column.
    /// </summary>
    [Entity("reg_binary_items")]
    public class RegBinaryItem
    {
        /// <summary>
        /// Gets or sets the identifier.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the payload. Never null.
        /// </summary>
        [Property("payload", Flags.None, 64)]
        public byte[] Payload { get; set; } = new byte[0];
    }
}
