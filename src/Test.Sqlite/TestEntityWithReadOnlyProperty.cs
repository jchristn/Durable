namespace Test.Sqlite
{
    /// <summary>
    /// Represents a test entity with a read-only property.
    /// </summary>
    public class TestEntityWithReadOnlyProperty
    {
        /// <summary>
        /// Gets or sets the unique identifier.
        /// </summary>
        public int Id { get; set; }

        /// <summary>
        /// Gets the read-only property value.
        /// </summary>
        public string ReadOnlyProperty { get; } = "Cannot be written";

        /// <summary>
        /// Initializes a new instance of the <see cref="TestEntityWithReadOnlyProperty"/> class.
        /// </summary>
        public TestEntityWithReadOnlyProperty()
        {
        }
    }
}