namespace Test.Shared
{
    using Durable.Sql;

    /// <summary>
    /// Concrete migration discovered by <see cref="SqlMigrator.AddMigrationsFromAssembly"/> in the discovery test.
    /// Creates the discovered-items table.
    /// </summary>
    public class MigDiscoveredFirst : Migration
    {
        /// <inheritdoc />
        public override string Id => "20261005_0001_DiscoveredFirst";

        /// <inheritdoc />
        public override string? Description => "Create discovered items table";

        /// <inheritdoc />
        public override void Up(MigrationContext context)
        {
            context.ExecuteSql("CREATE TABLE " + context.Dialect.QuoteIdentifier("mig_discovered") + " (" + context.Dialect.QuoteIdentifier("id") + " INT NOT NULL PRIMARY KEY)");
        }
    }
}
