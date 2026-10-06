namespace Test.Aot
{
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// Owner of a collection navigation whose target (<see cref="ShelfItem"/>) is deliberately never referenced through an
    /// annotated path, to verify the trimming diagnostic.
    /// </summary>
    [Entity("shelves")]
    public class Shelf
    {
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        [Property("name", Flags.String, 50)]
        public string Name { get; set; } = string.Empty;

        [InverseNavigationProperty("ShelfId")]
        public List<ShelfItem> Items { get; set; } = new List<ShelfItem>();
    }
}
