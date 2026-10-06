namespace Test.Shared.CliFixtures.Migrations
{
    using Durable.Sql;

    /// <summary>
    /// Second CLI fixture migration: adds a nullable note column to cli_items.
    /// </summary>
    public class CliMigration002AddNote : Migration
    {
        /// <summary>
        /// Gets the migration identifier.
        /// </summary>
        public override string Id => "20260101000002_AddNote";

        /// <summary>
        /// Adds the column.
        /// </summary>
        /// <param name="context">Migration context.</param>
        public override void Up(MigrationContext context)
        {
            ISqlDialect d = context.Dialect;
            context.ExecuteSql("ALTER TABLE " + d.QuoteIdentifier("cli_items") + " ADD " + d.QuoteIdentifier("note") + " VARCHAR(200) NULL");
        }

        /// <summary>
        /// Drops the column.
        /// </summary>
        /// <param name="context">Migration context.</param>
        public override void Down(MigrationContext context)
        {
            context.ExecuteSql(context.Dialect.DropColumnSql("cli_items", "note"));
        }
    }
}
