namespace Test.Shared.CliFixtures.Migrations
{
    using Durable.Sql;

    /// <summary>
    /// Third CLI fixture migration: indexes cli_items.name.
    /// </summary>
    public class CliMigration003IndexName : Migration
    {
        /// <summary>
        /// Gets the migration identifier.
        /// </summary>
        public override string Id => "20260101000003_IndexName";

        /// <summary>
        /// Creates the index.
        /// </summary>
        /// <param name="context">Migration context.</param>
        public override void Up(MigrationContext context)
        {
            context.ExecuteSql(context.Dialect.CreateIndexSql("idx_cli_items_name", "cli_items", new[] { "name" }, false, null));
        }

        /// <summary>
        /// Drops the index.
        /// </summary>
        /// <param name="context">Migration context.</param>
        public override void Down(MigrationContext context)
        {
            context.ExecuteSql(context.Dialect.DropIndexSql("idx_cli_items_name", "cli_items"));
        }
    }
}
