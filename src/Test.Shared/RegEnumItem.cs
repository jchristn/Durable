namespace Test.Shared
{
    using Durable;

    /// <summary>
    /// Entity with one enum stored as text (the default) and one stored as an integer.
    /// </summary>
    [Entity("reg_enum_items")]
    public class RegEnumItem
    {
        /// <summary>
        /// Gets or sets the identifier.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the name. Never null.
        /// </summary>
        [Property("name", Flags.String, 50)]
        public string Name { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the status, stored as the member name.
        /// </summary>
        [Property("status", Flags.String, 20)]
        public RegStatus Status { get; set; }

        /// <summary>
        /// Gets or sets the rank, stored as an integer.
        /// </summary>
        [Property("rank", Flags.Integer)]
        public RegStatus Rank { get; set; }

        /// <summary>
        /// Gets or sets the score.
        /// </summary>
        [Property("score")]
        public int Score { get; set; }
    }
}
