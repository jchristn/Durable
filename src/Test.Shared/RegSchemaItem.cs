namespace Test.Shared
{
    using Durable;

    /// <summary>
    /// Entity mapped to a schema-qualified table (PostgreSQL and SQL Server).
    /// </summary>
    [Entity("durable_reg.reg_schema_items")]
    public class RegSchemaItem
    {
        /// <summary>
        /// Gets or sets the identifier.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the name. Never null.
        /// </summary>
        [Property("name", Flags.String, 50)]
        public string Name { get; set; } = string.Empty;
    }
}
