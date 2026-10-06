namespace Test.Aot
{
    using Durable;

    /// <summary>
    /// Navigation target only reachable through <see cref="Shelf.Items"/>. Never use it as a repository type argument here.
    /// </summary>
    [Entity("shelf_items")]
    public class ShelfItem
    {
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        [Property("shelf_id")]
        public int ShelfId { get; set; }

        [Property("label", Flags.String, 50)]
        public string Label { get; set; } = string.Empty;
    }
}
