namespace Test.Aot
{
    using Durable;

    /// <summary>
    /// Category (many-to-many with <see cref="Author"/>).
    /// </summary>
    [Entity("categories")]
    public class Category
    {
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        [Property("name", Flags.String, 100)]
        public string Name { get; set; } = string.Empty;
    }
}
