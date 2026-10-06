namespace Test.Shared
{
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// LiteDB test entity: the owner of <see cref="LiteDbNote"/>s.
    /// </summary>
    [Entity("ldb_owners")]
    public class LiteDbOwner
    {
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        [Property("name", Flags.String, 100)]
        public string Name { get; set; } = string.Empty;

        [InverseNavigationProperty("OwnerId")]
        public List<LiteDbNote> Notes { get; set; } = new List<LiteDbNote>();
    }
}
