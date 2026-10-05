# Durable Connection Management

Durable 0.3 does not pool connections itself. Every SQL provider opens connections from its ADO.NET driver, and the
driver's pool (Microsoft.Data.Sqlite, Npgsql, MySqlConnector, Microsoft.Data.SqlClient) does the pooling. Earlier
versions layered a custom `ConnectionPool` on top. That layer added overhead, leaked tracking entries and disposed shared
factories, so it has been removed.

## Components

| Type | Package | Role |
|---|---|---|
| `IConnectionFactory` | Durable.Sql | `OpenConnection()` / `OpenConnectionAsync()` return an **open** connection that the caller disposes. |
| `ConnectionFactory` | Durable.Sql | Base class with an optional `MaxConcurrentConnections` cap and `AcquireTimeout`. |
| `SqliteConnectionFactory` | Durable.Sqlite | Keeps in-memory databases alive for the factory's lifetime; `:memory:` becomes a private shared-cache database. |
| `PostgresConnectionFactory` | Durable.Postgres | Wraps an `NpgsqlDataSource` (yours, or one it creates). |
| `MySqlConnectionFactory` | Durable.MySql | Wraps a `MySqlDataSource` (yours, or one it creates). |
| `SqlServerConnectionFactory` | Durable.SqlServer | Creates `SqlConnection` instances. |

## How repositories use connections

- **No transaction:** each operation opens a connection, runs, and disposes it, which returns it to the driver pool.
  Streaming reads (`ReadMany`, `ReadManyAsync`, `ExecuteAsyncEnumerable`) keep the connection until enumeration finishes
  or the enumerator is disposed.
- **Explicit transaction:** operations given an `ITransaction` run on that transaction's connection, which is not closed
  per operation.
- **Ambient transaction:** while a `TransactionScope` is current on the async flow, operations without an explicit
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
PostgresConnectionFactory factory = new PostgresConnectionFactory(connectionString);
PostgresRepository<Person> people = new PostgresRepository<Person>(factory);
PostgresRepository<Order> orders = new PostgresRepository<Order>(factory);
// ...
factory.Dispose(); // at shutdown
```

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
using ISqlTransaction tx = repository.BeginTransaction();
repository.Create(order, tx);
lineRepository.CreateMany(lines, tx);   // same provider, same connection
tx.Commit();                            // dispose without commit rolls back
```

`ISqlTransaction` exposes `Connection`, `Transaction`, and `CreateSavepoint()`.

Async ambient scopes work across `await`:

```csharp
using (TransactionScope scope = await TransactionScope.CreateAsync(repository))
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
