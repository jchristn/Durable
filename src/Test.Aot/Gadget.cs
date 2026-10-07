namespace Test.Aot
{
    /// <summary>
    /// Entity with no Durable attributes, mapped through <see cref="ExternalMappingSource"/>.
    /// </summary>
    [ExternalTable("gadgets")]
    public sealed class Gadget
    {
        [ExternalColumn("gadget_id", Key = true)]
        public int Id { get; set; }

        [ExternalColumn("gadget_name")]
        public string Name { get; set; } = string.Empty;

        [ExternalColumn("isbn", Isbn = true)]
        public Isbn? Isbn { get; set; }

        public string Scratch { get; set; } = string.Empty;
    }
}
