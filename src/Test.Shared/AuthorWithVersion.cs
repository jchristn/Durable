namespace Test.Sqlite
{
    using Durable;

    /// <summary>
    /// Represents an author entity with optimistic concurrency control using a version column
    /// </summary>
    [Entity("authors")]
    public class AuthorWithVersion
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
        
        /// <summary>
        /// Gets or sets the version number for optimistic concurrency control
        /// </summary>
        [Property("version")]
        [VersionColumn(VersionColumnType.Integer)]
        public int Version { get; set; }
    }
}