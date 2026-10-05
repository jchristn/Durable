namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable.Sqlite;
    using Xunit;

    /// <summary>
    /// SQLite connections from <see cref="SqliteConnectionFactory"/> must tolerate concurrent writers: other connections
    /// wait instead of failing, for shared in-memory databases and for files. (Shared-cache mode surfaces table-lock conflicts from Microsoft.Data.Sqlite as
    /// ArgumentOutOfRangeException; ":memory:" now maps to the memdb VFS, and named databases should use it too.)
    /// </summary>
    public class SqliteInMemoryConcurrencyTests
    {
        #region Public-Methods

        /// <summary>
        /// Readers on other connections wait for a writer holding an immediate transaction, then see its committed table.
        /// </summary>
        /// <param name="connectionString">In-memory connection string form.</param>
        [Theory]
        [InlineData("Data Source=:memory:")]
        [InlineData("Data Source=file:/DurableConcurrencyTest?vfs=memdb")]
        public async Task ReadersWaitForConcurrentWriter(string connectionString)
        {
            using SqliteConnectionFactory factory = new SqliteConnectionFactory(connectionString);
            Assert.True(factory.IsInMemory);
            List<Exception> errors = new List<Exception>();

            for (int round = 0; round < 10; round++)
            {
                string table = "conc_" + round.ToString(System.Globalization.CultureInfo.InvariantCulture);
                Task writer = Task.Run(async () =>
                {
                    await using DbConnection connection = await factory.OpenConnectionAsync(CancellationToken.None);
                    await using DbTransaction transaction = await connection.BeginTransactionAsync();
                    await using (DbCommand create = connection.CreateCommand())
                    {
                        create.Transaction = transaction;
                        create.CommandText = "CREATE TABLE " + table + " (x INTEGER)";
                        await create.ExecuteNonQueryAsync();
                    }

                    await Task.Delay(100);
                    await transaction.CommitAsync();
                });

                Task reader = Task.Run(async () =>
                {
                    for (int i = 0; i < 10; i++)
                    {
                        try
                        {
                            await using DbConnection connection = await factory.OpenConnectionAsync(CancellationToken.None);
                            await using DbCommand query = connection.CreateCommand();
                            query.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '" + table + "'";
                            await query.ExecuteScalarAsync();
                        }
                        catch (Exception ex)
                        {
                            lock (errors) errors.Add(ex);
                        }

                        await Task.Delay(10);
                    }
                });

                await Task.WhenAll(writer, reader);

                await using DbConnection check = await factory.OpenConnectionAsync(CancellationToken.None);
                await using DbCommand exists = check.CreateCommand();
                exists.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '" + table + "'";
                Assert.Equal(1L, Convert.ToInt64(await exists.ExecuteScalarAsync()));
            }

            Assert.True(errors.Count == 0, "Concurrent readers failed: " + (errors.Count == 0 ? string.Empty : errors[0].GetType().Name + ": " + errors[0].Message));
        }

        /// <summary>
        /// Parallel writers and readers on a file database wait for each other's locks (the factory sets
        /// <c>PRAGMA busy_timeout</c>) instead of failing with "database is locked".
        /// </summary>
        [Fact]
        public async Task ConcurrentWritersOnFileDatabaseWait()
        {
            string path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "durable-busy-" + Guid.NewGuid().ToString("N") + ".db");
            try
            {
                using (SqliteConnectionFactory factory = new SqliteConnectionFactory("Data Source=" + path))
                {
                    Assert.Equal(30000, factory.BusyTimeoutMilliseconds);
                    await using (DbConnection setup = await factory.OpenConnectionAsync(CancellationToken.None))
                    await using (DbCommand create = setup.CreateCommand())
                    {
                        create.CommandText = "CREATE TABLE busy_items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)";
                        await create.ExecuteNonQueryAsync();
                    }

                    List<Task> tasks = new List<Task>();
                    for (int i = 0; i < 200; i++)
                    {
                        int index = i;
                        tasks.Add(Task.Run(async () =>
                        {
                            await using DbConnection connection = await factory.OpenConnectionAsync(CancellationToken.None);
                            await using DbCommand command = connection.CreateCommand();
                            if (index % 2 == 0)
                            {
                                command.CommandText = "INSERT INTO busy_items (name) VALUES ('x') RETURNING id";
                                await command.ExecuteScalarAsync();
                            }
                            else
                            {
                                command.CommandText = "SELECT name FROM busy_items";
                                await using DbDataReader reader = await command.ExecuteReaderAsync();
                                while (await reader.ReadAsync()) { }
                            }
                        }));
                    }

                    await Task.WhenAll(tasks);

                    await using DbConnection check = await factory.OpenConnectionAsync(CancellationToken.None);
                    await using DbCommand count = check.CreateCommand();
                    count.CommandText = "SELECT COUNT(*) FROM busy_items";
                    Assert.Equal(100L, Convert.ToInt64(await count.ExecuteScalarAsync()));
                }
            }
            finally
            {
                Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
                if (System.IO.File.Exists(path)) System.IO.File.Delete(path);
            }
        }

        #endregion
    }
}
