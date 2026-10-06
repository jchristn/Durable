namespace Test.Shared.CliFixtures.Migrations
{
    using Durable.Sql;

    /// <summary>
    /// First CLI fixture migration: creates the cli_items table. Discovered by the durable tool tests through
    /// --migrations-namespace Test.Shared.CliFixtures.Migrations.
    /// </summary>
    public class CliMigration001CreateItems : Migration
    {
        /// <summary>
        /// Gets the migration identifier.
        /// </summary>
        public override string Id => "20260101000001_CreateItems";

        /// <summary>
        /// Creates the table.
        /// </summary>
        /// <param name="context">Migration context.</param>
        public override void Up(MigrationContext context)
        {
            ISqlDialect d = context.Dialect;
            context.ExecuteSql("CREATE TABLE " + d.QuoteIdentifier("cli_items") + " (" + d.QuoteIdentifier("id") + " INT NOT NULL PRIMARY KEY, " +
                d.QuoteIdentifier("name") + " VARCHAR(100) NOT NULL)");
        }

        /// <summary>
        /// Drops the table.
        /// </summary>
        /// <param name="context">Migration context.</param>
        public override void Down(MigrationContext context)
        {
            context.ExecuteSql("DROP TABLE " + context.Dialect.QuoteIdentifier("cli_items"));
        }
    }
}
