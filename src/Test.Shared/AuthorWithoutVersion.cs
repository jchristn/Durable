namespace Test.Sqlite
{
    using Durable;

    /// <summary>
    /// Represents an author entity without version-based optimistic concurrency control
    /// </summary>
    [Entity("authors")]
    public class AuthorWithoutVersion
    {
        /// <summary>
        /// Gets or sets the unique identifier for the author
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }
        
        /// <summary>
        /// Gets or sets the name of the author
        /// </summary>
        [Property("name", Flags.String)]
        public string Name { get; set; }
        
        /// <summary>
        /// Gets or sets the optional company identifier that the author is associated with
        /// </summary>
        [Property("company_id")]
        public int? CompanyId { get; set; }
    }
}