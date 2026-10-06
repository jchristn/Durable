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
            context.ExecuteSql("CREATE TABLE ledger (id INTEGER PRIMARY KEY AUTOINCREMENT, entry TEXT NOT NULL)");
            context.ExecuteSql("INSERT INTO ledger (entry) VALUES (@p0)", "opening");
        }

        public override void Down(MigrationContext context)
        {
            context.ExecuteSql("DROP TABLE ledger");
        }
    }
}
