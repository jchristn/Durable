# Durable Connection Management

Since 0.3, Durable does not pool connections itself. Every SQL provider opens connections from its ADO.NET driver, and the
driver's pool (Microsoft.Data.Sqlite, Npgsql, MySqlConnector, Microsoft.Data.SqlClient) does the pooling. Earlier
versions layered a custom `ConnectionPool` on top. That layer added overhead, leaked tracking entries and disposed shared
factories, so it has been removed.

## Components

| Type | Package | Role |
|---|---|---|
| `IConnectionFactory` | Durable.Sql | `OpenConnection()` / `OpenConnectionAsync()` return an **open** connection that the caller disposes. |
| `ConnectionFactory` | Durable.Sql | Base class with an optional `MaxConcurrentConnections` cap and `AcquireTimeout`. |
| `SqliteConnectionFactory` | Durable.Sqlite | Sets `PRAGMA busy_timeout` on every connection (`BusyTimeoutMilliseconds`, default 30 s) so concurrent writers wait instead of failing with "database is locked". `:memory:` becomes a private in-memory database on SQLite's memdb VFS (since 0.4.0; previously shared-cache mode) that lives as long as the factory. |
| `PostgresConnectionFactory` | Durable.Postgres | Wraps an `NpgsqlDataSource`: yours, or one it builds from a connection string or `PostgresRepositorySettings`. |
| `MySqlConnectionFactory` | Durable.MySql | Wraps a `MySqlDataSource`: yours, or one it builds from a connection string or `MySqlRepositorySettings`. |
| `SqlServerConnectionFactory` | Durable.SqlServer | Creates `SqlConnection` instances from a connection string or `SqlServerRepositorySettings`. |

Every factory takes a connection string or the provider's settings object (PostgreSQL and MySQL also a driver data source), plus an optional `maxConcurrentConnections`. Factories are `IDisposable` and `IAsyncDisposable`.

## How repositories use connections

- **No transaction:** each operation opens a connection, runs, and disposes it, which returns it to the driver pool.
  Streaming reads (`ReadMany`, `ReadManyAsync`, `ExecuteAsyncEnumerable`) keep the connection until enumeration finishes
  or the enumerator is disposed.
- **Explicit transaction:** operations given an `ITransaction` run on that transaction's connection, which is not closed
  per operation.
- **Ambient transaction:** while a Durable `AmbientTransactionScope` is current on the async flow, operations without an explicit
  transaction use it, as long as it belongs to the same provider.
- **Includes:** loaded with follow-up queries on the same connection after the root query completes. When streaming with
  includes outside a transaction, a second connection loads each batch.

## Ownership

| Constructor | Who disposes the factory |
|---|---|
| `new XRepository<T>(connectionString)` | The repository. |
| `new XRepository<T>(settings)` | The repository. |
| `new XRepository<T>(connectionFactory)` | **You.** Disposing the repository leaves the factory alone, so one factory can serve many repositories. |

For long-running applications, create one factory per database and share it:

```csharp
await using PostgresConnectionFactory factory = new PostgresConnectionFactory(connectionString);   // disposed at shutdown
PostgresRepository<Person> people = new PostgresRepository<Person>(factory);
PostgresRepository<Order> orders = new PostgresRepository<Order>(factory);

// Or from strongly-typed settings
PostgresConnectionFactory fromSettings = new PostgresConnectionFactory(new PostgresRepositorySettings
{
    Hostname = "localhost", Database = "app", Username = "app", Password = "secret", MaxPoolSize = 50
});
```

### SQLite and the driver pool

Disposing a `SqliteConnectionFactory` releases only the private database it generated for `:memory:`. It does not clear
Microsoft.Data.Sqlite's connection pool for a file or a named in-memory database, because other factories and
repositories may be using the same database (before 0.5.0 it did, which failed their in-flight queries with
`ObjectDisposedException` 'SQLitePCL.sqlite3'). To release a file (for example before deleting it, which matters on
Windows) or a named in-memory database, call `SqliteConnection.ClearPool(connection)` or `SqliteConnection.ClearAllPools()`
after disposing the repositories that use it.

## Limiting concurrency

Pool size is a driver setting (for example `Maximum Pool Size=50`). To cap how many connections Durable itself holds
open, for example to protect a small server, pass `maxConcurrentConnections`:

```csharp
SqlServerConnectionFactory factory = new SqlServerConnectionFactory(connectionString, maxConcurrentConnections: 20)
{
    AcquireTimeout = TimeSpan.FromSeconds(10)
};
```

When the cap is reached, `OpenConnection` waits up to `AcquireTimeout` and then throws `TimeoutException`. A slot is
released when the connection is closed or disposed.

## Transactions

```csharp
using ISqlTransaction tx = repository.BeginTransaction();   // or: await using ISqlTransaction tx = await repository.BeginTransactionAsync();
repository.Create(order, tx);
lineRepository.CreateMany(lines, tx);   // same provider, same connection
tx.Commit();                            // dispose without commit rolls back
```

`ISqlTransaction` exposes `Connection`, `Transaction`, and `CreateSavepoint()` / `CreateSavepointAsync()`. Savepoints are not disposable: call `Rollback`/`Release` (or their async forms) explicitly.

Async ambient scopes work across `await` (dispose them with `await using` so an uncompleted scope rolls back asynchronously). Durable does not take part in `System.Transactions`: it never reads `Transaction.Current` or enlists, so a `System.Transactions.TransactionScope` does not make Durable operations atomic (a driver that auto-enlists may still enlist a connection Durable opens; do not rely on it).

```csharp
await using (AmbientTransactionScope scope = await AmbientTransactionScope.CreateAsync(repository))
{
    await repository.CreateAsync(a);      // joins the scope
    await otherRepository.CreateAsync(b); // joins the scope
    await scope.CompleteAsync();
}
```

### Sharing a transaction with Dapper or EF Core

Wrap a connection and transaction you manage yourself. Durable runs on them and never commits, rolls back or disposes them.

```csharp
await using NpgsqlConnection connection = await dataSource.OpenConnectionAsync();
await using NpgsqlTransaction transaction = await connection.BeginTransactionAsync();

await connection.ExecuteAsync("UPDATE accounts SET ...", transaction: transaction);  // Dapper
await repository.CreateAsync(auditEntry, SqlTransactionContext.Wrap(connection, transaction, PostgresDialect.Default));

await transaction.CommitAsync();
```

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Driver pool timeout | A streamed `ReadMany` enumerator was not fully enumerated or disposed. | Enumerate to the end, call `.ToList()`, or use `using` on the enumerator. |
| Driver pool timeout | Transactions held open across slow work. | Keep transactions short; dispose them in `using`. |
| `TimeoutException` from Durable | `MaxConcurrentConnections` is too low for the workload. | Raise the cap or lower concurrency. |
| `ArgumentException` "transaction cannot span database providers" | A transaction from one provider was passed to another provider's repository. | Use one transaction per database. |
| In-memory SQLite data disappears | The factory that owned the database was disposed. | Share one `SqliteConnectionFactory` for the database's lifetime. |
| SQLite file cannot be deleted ("in use", Windows) | The driver pool still holds connections to it; disposing factories does not clear the pool. | Call `SqliteConnection.ClearAllPools()` (or `ClearPool`) first. |
| "database is locked" (SQLite) | Another connection held the write lock longer than `BusyTimeoutMilliseconds`. | Keep transactions short or raise the timeout. |
