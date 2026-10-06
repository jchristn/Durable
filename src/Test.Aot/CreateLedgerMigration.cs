namespace Test.Aot
{
    using Durable.Sql;

    /// <summary>
    /// Versioned migration: creates a ledger table and seeds it (Down drops it).
    /// </summary>
    public sealed class CreateLedgerMigration : Migration
    {
        public override string Id => "202610050001_CreateLedger";

        public override void Up(MigrationContext context)
        {
            context.ExecuteSqlRaw("CREATE TABLE ledger (id INTEGER PRIMARY KEY AUTOINCREMENT, entry TEXT NOT NULL)");
            context.ExecuteSqlRaw("INSERT INTO ledger (entry) VALUES ({0})", new object?[] { "opening" });
        }

        public override void Down(MigrationContext context)
        {
            context.ExecuteSqlRaw("DROP TABLE ledger");
        }
    }
}
