namespace Test.Shared
{
    using Durable;

    /// <summary>
    /// Entity whose names differ only by case, accent or LIKE wildcards, used to verify <see cref="StringMatchMode"/>.
    /// </summary>
    [Entity("str_match_items")]
    public class StrMatchItem
    {
        /// <summary>
        /// Gets or sets the identifier.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the name. May be null.
        /// </summary>
        [Property("name", Flags.String, 64)]
        public string? Name { get; set; }
    }
}
