namespace Test.Sqlite
{
    /// <summary>
    /// Represents a test entity for unit testing purposes.
    /// </summary>
    public class TestEntity
    {
        /// <summary>
        /// Gets or sets the unique identifier.
        /// </summary>
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the name.
        /// </summary>
        public string Name { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the value.
        /// </summary>
        public int Value { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether this instance is active.
        /// </summary>
        public bool IsActive { get; set; }

        /// <summary>
        /// Gets or sets the description.
        /// </summary>
        public string? Description { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="TestEntity"/> class.
        /// </summary>
        public TestEntity()
        {
        }
    }
}