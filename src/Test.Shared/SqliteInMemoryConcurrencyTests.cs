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
        /// A native connection that Microsoft.Data.Sqlite pooled while still inside a transaction (here one started with raw
        /// SQL, so the driver does not know about it) is rolled back when the factory leases it again: BeginTransaction
        /// works and the abandoned write is gone. Without this, the next lease failed with "cannot start a transaction
        /// within a transaction" (the intermittent ParallelBatchOperations_ShouldHandleConcurrency failure).
        /// </summary>
        /// <param name="useAsync">True to lease with OpenConnectionAsync, false with OpenConnection.</param>
        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task PooledConnectionLeftInTransactionIsRolledBackOnLease(bool useAsync)
        {
            string name = "DurableStaleTxn" + (useAsync ? "Async" : "Sync");
            using SqliteConnectionFactory factory = new SqliteConnectionFactory("Data Source=file:/" + name + "?vfs=memdb");
            await using (DbConnection setup = await LeaseAsync(factory, useAsync))
            {
                await ExecuteAsync(setup, null, "CREATE TABLE stale_txn (x INTEGER)");
            }

            await using (DbConnection abandoned = await LeaseAsync(factory, useAsync))
            {
                await ExecuteAsync(abandoned, null, "BEGIN");
                await ExecuteAsync(abandoned, null, "INSERT INTO stale_txn VALUES (1)");
            }

            for (int i = 0; i < 3; i++)
            {
                await using DbConnection connection = await LeaseAsync(factory, useAsync);
                await using DbTransaction transaction = await connection.BeginTransactionAsync();
                await using DbCommand count = connection.CreateCommand();
                count.Transaction = transaction;
                count.CommandText = "SELECT COUNT(*) FROM stale_txn";
                Assert.Equal(0L, Convert.ToInt64(await count.ExecuteScalarAsync()));
                await transaction.CommitAsync();
            }
        }

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
                using (Microsoft.Data.Sqlite.SqliteConnection connection = new Microsoft.Data.Sqlite.SqliteConnection("Data Source=" + path))
                {
                    Microsoft.Data.Sqlite.SqliteConnection.ClearPool(connection);
                }

                if (System.IO.File.Exists(path)) System.IO.File.Delete(path);
            }
        }

        /// <summary>
        /// Disposing a factory must not clear the driver's connection pool for a connection string that other factories
        /// share. It used to: clearing the shared pool raced with queries in flight on other factories and failed them with
        /// <see cref="ObjectDisposedException"/> ("SQLitePCL.sqlite3"). Observed deterministically through the pooled native
        /// handle, which survives another factory's disposal only when the pool is left alone.
        /// </summary>
        [Fact]
        public async Task DisposingAFactoryKeepsThePoolOfASharedConnectionString()
        {
            string connectionString = "Data Source=file:/DurableFactoryDisposeTest" + Guid.NewGuid().ToString("N") + "?vfs=memdb";
            using SqliteConnectionFactory factory = new SqliteConnectionFactory(connectionString);

            SQLitePCL.sqlite3 before;
            await using (DbConnection first = await factory.OpenConnectionAsync(CancellationToken.None))
            {
                before = ((Microsoft.Data.Sqlite.SqliteConnection)first).Handle!;
            }

            SqliteConnectionFactory other = new SqliteConnectionFactory(connectionString);
            await using (DbConnection used = await other.OpenConnectionAsync(CancellationToken.None))
            {
            }

            other.Dispose();

            await using (DbConnection second = await factory.OpenConnectionAsync(CancellationToken.None))
            {
                Assert.Same(before, ((Microsoft.Data.Sqlite.SqliteConnection)second).Handle);
            }
        }

        /// <summary>
        /// Disposing a factory over ":memory:" releases its private database: a new factory starts empty.
        /// </summary>
        [Fact]
        public async Task DisposingAPrivateMemoryFactoryReleasesItsDatabase()
        {
            string connectionString;
            using (SqliteConnectionFactory factory = new SqliteConnectionFactory("Data Source=:memory:"))
            {
                connectionString = factory.ConnectionString;
                await using DbConnection connection = await factory.OpenConnectionAsync(CancellationToken.None);
                await using DbCommand create = connection.CreateCommand();
                create.CommandText = "CREATE TABLE private_items (id INTEGER PRIMARY KEY)";
                await create.ExecuteNonQueryAsync();
            }

            await using Microsoft.Data.Sqlite.SqliteConnection reopened = new Microsoft.Data.Sqlite.SqliteConnection(connectionString);
            await reopened.OpenAsync();
            await using DbCommand check = reopened.CreateCommand();
            check.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE name = 'private_items'";
            Assert.Equal(0L, Convert.ToInt64(await check.ExecuteScalarAsync()));
        }

        #endregion

        #region Private-Methods

        private static async Task<DbConnection> LeaseAsync(SqliteConnectionFactory factory, bool useAsync)
        {
            return useAsync ? await factory.OpenConnectionAsync(CancellationToken.None) : factory.OpenConnection();
        }

        private static async Task ExecuteAsync(DbConnection connection, DbTransaction? transaction, string sql)
        {
            await using DbCommand command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = sql;
            await command.ExecuteNonQueryAsync();
        }

        #endregion
    }
}
