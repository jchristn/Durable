namespace Test.Shared
{
    using Durable.Sql;

    /// <summary>
    /// Concrete migration discovered by <see cref="SqlMigrator.AddMigrationsFromAssembly"/> in the discovery test.
    /// Seeds one row into the discovered-items table.
    /// </summary>
    public class MigDiscoveredSecond : Migration
    {
        /// <inheritdoc />
        public override string Id => "20261005_0002_DiscoveredSecond";

        /// <inheritdoc />
        public override void Up(MigrationContext context)
        {
            context.ExecuteSql("INSERT INTO " + context.Dialect.QuoteIdentifier("mig_discovered") + " (" + context.Dialect.QuoteIdentifier("id") + ") VALUES (@p0)", 7);
        }
    }
}
