namespace Test.Aot
{
    using System;
    using System.IO;
    using System.Linq;
    using System.Threading.Tasks;
    using Microsoft.Data.Sqlite;
    using Durable;
    using Durable.Sqlite;

    /// <summary>
    /// Entity mapping source checks: a class with no Durable attributes, mapped by <see cref="ExternalMappingSource"/>,
    /// round-trips through SQLite (including a value converter the source attaches in code).
    /// </summary>
    internal static class MappingSourceScenario
    {
        public static async Task RunAsync(CheckRunner runner)
        {
            DurableMapping.Register<Gadget>(new ExternalMappingSource());
            runner.Check("[mapping] Metadata comes from the registered source", () =>
            {
                EntityMetadata metadata = EntityMetadata.For<Gadget>();
                return metadata.MappingSource is ExternalMappingSource
                    && metadata.TableName == "gadgets"
                    && metadata.KeyColumns.Single().Name == "gadget_id"
                    && metadata.FindColumnByProperty("Isbn")!.Converter is IsbnConverter
                    && metadata.FindColumnByProperty("Scratch") == null;
            });

            string path = Path.Combine(Path.GetTempPath(), "durable-aot-mapping-" + Guid.NewGuid().ToString("N") + ".db");
            try
            {
                using SqliteConnectionFactory factory = new SqliteConnectionFactory("Data Source=" + path);
                using SqliteRepository<Gadget> gadgets = new SqliteRepository<Gadget>(factory);
                gadgets.InitializeTable(typeof(Gadget));
                await runner.CheckAsync("[mapping] Source-mapped type round-trips through SQLite", async () =>
                {
                    Gadget created = await gadgets.CreateAsync(new Gadget { Name = "Lever", Isbn = new Isbn("978-1"), Scratch = "x" }).ConfigureAwait(false);
                    Gadget? read = await gadgets.ReadByIdAsync(created.Id).ConfigureAwait(false);
                    string? stored = gadgets.ExecuteScalarRaw<string>("SELECT isbn FROM gadgets WHERE gadget_id = {0}", new object?[] { created.Id });
                    return created.Id > 0
                        && read?.Name == "Lever"
                        && read.Isbn?.Value == "978-1"
                        && read.Scratch == string.Empty
                        && stored == "ISBN:978-1"
                        && gadgets.ReadMany(g => g.Name == "Lever").Count() == 1;
                }).ConfigureAwait(false);
            }
            finally
            {
                SqliteConnection.ClearAllPools();
                try
                {
                    File.Delete(path);
                }
                catch (IOException)
                {
                }
            }
        }
    }
}
