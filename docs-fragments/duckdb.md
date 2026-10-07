# duckdb: Durable.DuckDb (v0.7.0)

Driver: `DuckDB.NET.Data.Full` 1.5.6 (bundles the DuckDB 1.5.6 engine and its native libraries for linux-x64,
linux-arm64, osx, win-x64 and win-arm64). In-process, no server or container. Test `--type` name: `duckdb`.
Durable.Tool provider name: `duckdb`. `RepositoryType.DuckDb` (`"duckdb"`, `"DuckDB"`); OpenTelemetry `db.system`: `duckdb`.

## README sections

### 1. New section "DuckDB" (place it at the end of "Connecting to a SQL Database", after the docker snippet)

The code below was compiled and run against Durable.DuckDb (it assumes the `Person`/`Author` entities of this guide).

````markdown
### DuckDB

DuckDB is an embedded, in-process analytical database: there is no server, and `Durable.DuckDb` loads the engine
(bundled by DuckDB.NET.Data.Full) into your process. The data source is a database file, `:memory:` or
`:memory:?cache=shared`.

```csharp
using Durable.DuckDb;

// Database file: the repository creates and owns its connection factory
DuckDbRepository<Person> people = new DuckDbRepository<Person>("Data Source=analytics.duckdb");

// Settings object (file, private in-memory, or the process-wide shared in-memory database)
DuckDbRepository<Person> fromSettings = new DuckDbRepository<Person>(DuckDbRepositorySettings.ForFile("analytics.duckdb"));
DuckDbRepositorySettings tuned = new DuckDbRepositorySettings { DataSource = "analytics.duckdb", Threads = 4, MemoryLimit = "2GB" };

// In-memory: share one factory. The database lives as long as the factory and only its repositories see it.
using DuckDbConnectionFactory memory = new DuckDbConnectionFactory(DuckDbRepositorySettings.ForInMemory());
DuckDbRepository<Person> inMemoryPeople = new DuckDbRepository<Person>(memory);
DuckDbRepository<Author> inMemoryAuthors = new DuckDbRepository<Author>(memory);
inMemoryPeople.InitializeTable(typeof(Person));   // sequence-backed key: CREATE SEQUENCE + DEFAULT nextval(...)

// Bulk loading goes through the DuckDB Appender
List<Person> batch = Enumerable.Range(0, 10000).Select(i => new Person { FirstName = "P" + i, LastName = "L", Age = i % 90 }).ToList();
long inserted = await inMemoryPeople.BulkInsertAsync(batch);
```

How the connection factory keeps databases alive. DuckDB has no connection pool: a database instance exists while
at least one connection to it is open. `DuckDbConnectionFactory` opens one root connection on first use and keeps it
open until the factory is disposed; every connection it hands out shares that instance.

| Data source | Who sees the data | Lifetime |
|---|---|---|
| `:memory:` (`DuckDbRepositorySettings.ForInMemory()`) | Every repository on the same factory; nothing else (a raw connection or a second factory gets its own empty database) | Until the factory is disposed |
| `:memory:?cache=shared` (`ForSharedInMemory()`) | Every factory and raw connection in the process using that string | While any connection to it is open |
| A file path (`ForFile(path)`) | Every factory and connection in the process opened on the file | The file; the factory holds DuckDB's file lock (one read-write process at a time) until it is disposed |

A repository created from a connection string owns its factory, so `new DuckDbRepository<T>("Data Source=:memory:")`
is a private database for that one repository. Share one factory instead.

Concurrency. DuckDB uses optimistic multi-version concurrency control: transactions never wait for each other, and a
transaction that updates or deletes a row another concurrent transaction changed fails with
`TransactionContext Error: Conflict on update` (a `DuckDBException`) instead of blocking. Retry such failures where
several threads update the same rows; inserts of different rows do not conflict.

```csharp
for (int attempt = 1; ; attempt++)
{
    try
    {
        await inMemoryPeople.UpdateFieldAsync(p => p.Id == first.Id, p => p.Age, 42);
        break;
    }
    catch (DuckDBException e) when (attempt < 3 && e.Message.Contains("Conflict", StringComparison.OrdinalIgnoreCase))
    {
        await Task.Delay(10 * attempt);   // another transaction changed the row first: retry
    }
}
```

What differs from the other SQL providers:

- **Keys**: DuckDB has no identity columns. An auto-increment key gets a sequence named `{table}_{column}_seq` and
  `DEFAULT nextval(...)`; `INSERT ... RETURNING` returns the key. Creating the table recreates a sequence left behind
  by a dropped table of the same name, so a recreated table numbers from 1 again.
- **Savepoints are not supported** (DuckDB has no `SAVEPOINT`): `ISqlTransaction.CreateSavepoint(Async)` throws
  `NotSupportedException` (`ISqlDialect.SupportsSavepoints` is false). Use separate transactions.
- **Stored procedures**: not supported (DuckDB has macros, not procedures); the procedure APIs throw `NotSupportedException`.
- **String lengths**: DuckDB accepts `VARCHAR(n)` but neither stores nor enforces `n`, so strings are `VARCHAR`,
  `MaxLength` is not enforced, schema comparison reports no length differences and scaffolding cannot recover lengths
  (`ISqlDialect.SupportsStringMaxLength` is false, as on SQLite).
- **Migrations run statement by statement**: DuckDB can roll back DDL, but it cannot create an index in a transaction
  that already changed rows of the table (for example by adding a column with a default), nor drop a column in the
  transaction that dropped its index. Durable therefore runs migrations and schema sync without a wrapping transaction
  (`SupportsTransactionalDdl` is false, as on MySQL): a failed migration is not recorded, but its earlier statements stay
  applied (`MigrationException.MayBePartiallyApplied`).
- **Altering indexed tables**: DuckDB refuses to drop a column, make a column NOT NULL or change its type while the
  table has indexes. Schema sync (and `durable schema sync` / `migrations add`) wraps such operations: it drops the
  table's indexes, alters the table and re-creates the indexes that remain. Raw `ALTER TABLE` statements in your own
  migrations must do the same.
- **Migration lock**: DuckDB has no advisory locks. The lock is a row in `durable_migration_lock` (created on first
  use, excluded from `ReadTableNames`, name configurable on `DuckDbDialect`). A second migrator in the same process polls
  until the row is gone. A read-write DuckDB file can be opened by one process at a time, so a row left by a process
  that exited without releasing it is treated as stale and replaced.
- **Command timeout**: DuckDB.NET ignores `DbCommand.CommandTimeout`, so Durable enforces
  `SqlRepositoryOptions.CommandTimeoutSeconds` itself: it interrupts the command when the timeout elapses and throws
  `TimeoutException`. Without a configured timeout, commands run to completion. Cancellation tokens interrupt running
  statements.
- **Integer division** uses DuckDB's `//` (truncating like C#); `SUM` over integer columns returns `HUGEINT`, which the
  converter narrows to the requested type.
- **String matching**: DuckDB compares strings by code point (case- and accent-sensitive) and its `LIKE` is
  case-sensitive, so `StringMatchMode.Database` behaves like `Ordinal` unless you configure a default collation such as
  `nocase`; `Ordinal` and `IgnoreCase` apply `COLLATE "binary"` (with `lower()` for `IgnoreCase`).
- **Bulk insert** uses the DuckDB Appender when every value's type matches its column (always true for tables Durable
  creates; keys are drawn from the sequence first) and falls back to a prepared `INSERT` per row otherwise. Both run in
  the bulk insert's transaction.
````

### 2. "String Matching" section: replace the sentence listing the binary collations with

```markdown
The SQL dialects apply a binary collation for the non-default modes: `"C"` on PostgreSQL, `utf8mb4_bin` on MySQL, `Latin1_General_100_BIN2` on SQL Server, `"binary"` on DuckDB, and `BINARY` with `INSTR`/`SUBSTR` on SQLite, whose `LIKE` ignores collations. The collation names are constructor parameters of the MySQL, PostgreSQL, SQL Server and DuckDB dialects.
```

### 3. "Migrations" section: replace the two lock / transaction bullets with

```markdown
- Migrations run in ordinal `Id` order under a database lock (PostgreSQL advisory lock, SQL Server `sp_getapplock`, MySQL `GET_LOCK`, SQLite `BEGIN IMMEDIATE`, DuckDB a row in `durable_migration_lock`).
- On PostgreSQL, SQL Server and SQLite each migration commits together with its history row, so a failed migration is rolled back and not recorded. MySQL commits DDL implicitly, and DuckDB migrations run statement by statement (see [DuckDB](#duckdb)): a failed migration is not recorded but earlier statements stay applied (`MigrationException.MayBePartiallyApplied`), so keep MySQL and DuckDB migrations small and idempotent.
```

### 4. "Continuous Integration and Tests": add to the run-locally block, after the SQLite line

```bash
# DuckDB in process (no docker); add --filename <path> for a database file instead of the shared in-memory database
dotnet run --project src/Test.Automated/Test.Automated.csproj -f net8.0 -- --type duckdb
```

### 5. "Installation": add

```bash
dotnet add package Durable.DuckDb       # DuckDB (embedded, in-process)
```

### 6. "Command-Line Tool": first sentence becomes "... on SQLite, DuckDB, PostgreSQL, MySQL and SQL Server."

## README table rows

### Packages

| `Durable.DuckDb` | DuckDB provider (embedded, in-process analytics database) | Durable.Sql, DuckDB.NET.Data.Full 1.5.6 |

`Durable.Tool` row: "Depends on" becomes "the SQL providers" (it now also references Durable.DuckDb).

### Which package do I need?

| Use DuckDB (embedded analytics: a file or in memory, no server) | `Durable.DuckDb` |

### Requirements: databases

| DuckDB | 1.5.6 (bundled by DuckDB.NET.Data.Full) | DuckDB.NET.Data.Full 1.5.6 | Tested version; `RETURNING`, `ON CONFLICT`, sequences and `duckdb_*()` introspection are used |

CI sentence: "CI tests against SQLite and DuckDB (both bundled, in-process, on Linux, Windows and macOS), `postgres:16`, ..."

### Backends Compared (new "DuckDB" column, placed after SQLite)

| Row | DuckDB |
|---|---|
| Repository type | `DuckDbRepository<T>` |
| Storage | File or memory (in-process) |
| `Capabilities` | All |
| Full LINQ, Include, grouping, projections, aggregates | Yes |
| Where queries run | Database |
| Transactions | Database (optimistic MVCC: conflicting writers fail instead of waiting) |
| Savepoints | No |
| Upsert, `BatchUpdate`, `UpdateField` | Yes |
| `StringMatchMode.Database` behaves as | Collation (binary by default: exact, case-sensitive `LIKE`) |
| Raw SQL, `QueryMultiple` | Yes |
| Stored procedures | No |
| `BulkInsert` | DuckDB Appender (prepared inserts when column types differ) |
| Set operations, CTEs, window functions | Yes |
| `InitializeTable`, migrations, `durable` CLI | Yes (migrations run statement by statement) |
| SQL capture, interceptors, OpenTelemetry | Yes |
| Native AOT | Yes (verified on .NET 10; see Native AOT) |
| Best for | Embedded analytics, local data processing, tests against real SQL |

### Connecting: settings classes

| `DuckDbRepositorySettings` | `DataSource`, `AccessMode` (`DuckDBAccessMode`), `Threads`, `MemoryLimit`, `IsInMemory`; `ForInMemory()`, `ForSharedInMemory()`, `ForFile(path)` (host, port, user, password and database are not used) |

### Type Mapping (new "DuckDB" column, placed after SQLite)

| CLR type | DuckDB |
|---|---|
| `bool` | `BOOLEAN` |
| `byte` / `sbyte` | `UTINYINT` / `TINYINT` |
| `short` / `ushort` | `SMALLINT` / `USMALLINT` |
| `int` / `uint` | `INTEGER` / `UINTEGER` |
| `long` / `ulong` | `BIGINT` / `UBIGINT` |
| `float` / `double` | `FLOAT` / `DOUBLE` |
| `decimal` | `DECIMAL(38,10)` |
| `string` | `VARCHAR` (length not stored or enforced) |
| `char` | `VARCHAR` |
| `Guid` | `UUID` |
| `DateTime` | `TIMESTAMP` |
| `DateTimeOffset` | `TIMESTAMPTZ` (UTC) |
| `DateOnly` / `TimeOnly` | `DATE` / `TIME` |
| `TimeSpan` | `INTERVAL` |
| `byte[]` | `BLOB` |
| enum (default, by name) | `VARCHAR` |
| enum with `Flags.Integer` | `INTEGER` |
| `Flags.Json`, collections, complex objects | `JSON` |
| Auto-increment integer key | `DEFAULT nextval('{table}_{column}_seq')` (sequence created with the table) |

Add after the table: "On DuckDB, a value converter whose provider type is `System.Numerics.BigInteger` maps to `HUGEINT`."

### Native AOT: provider support

| `Durable.DuckDb` | Yes, verified end to end on .NET 10 (osx-arm64 locally, linux-x64 in CI); the test runs on .NET 9+. DuckDB.NET.Data is not trim-annotated: the AOT compiler prints summary warnings (IL2104/IL3053) for it, from its LIST/STRUCT/MAP readers, collection parameters and connection-string property descriptors, which Durable.DuckDb does not use (it maps scalar columns, binds scalar parameters and reads with `GetValue`). On .NET 8 the AOT compiler cannot summarize those warnings, so a warnings-as-errors .NET 8 AOT build needs them suppressed for DuckDB.NET.Data |

### Command-Line Tool: common options

| `--provider sqlite\|duckdb\|postgres\|mysql\|sqlserver`, `--connection "<string>"` | Database. Default: `DURABLE_PROVIDER` / `DURABLE_CONNECTION` environment variables, then `./durable.json` |

### Continuous Integration: jobs

| DuckDB | The CLI runner on DuckDB (in-process) on Linux, Windows and macOS, net8.0 and net10.0 |

### Troubleshooting and FAQ

| DuckDB: `TransactionContext Error: Conflict on update` | DuckDB's concurrency control is optimistic: a transaction that updates a row another open transaction changed fails instead of waiting. Retry the operation (see [DuckDB](#duckdb)), or serialize writers to the same rows. |
| DuckDB `:memory:` data disappears, or two repositories see different data | Each `DuckDbConnectionFactory` built from `:memory:` (and each repository created from a `:memory:` connection string) is its own database, alive while the factory lives. Share one factory, or use `:memory:?cache=shared` for one database per process. |
| DuckDB: `Dependency Error: Cannot alter entry ... because there are entries that depend on it` | DuckDB cannot drop a column, make one NOT NULL or change its type while the table has indexes. Schema sync handles this for you; in a hand-written migration, drop the indexes first and re-create them afterwards. |
| DuckDB: `Could not set lock on file` | A read-write DuckDB file is open in another process (including a still-running app or another factory's root connection). Dispose the factories (or repositories that own them) that use the file, or open it read-only (`AccessMode = DuckDBAccessMode.ReadOnly`) from the other processes. |
| DuckDB: `NotSupportedException` from `CreateSavepoint` | DuckDB has no savepoints; use separate transactions. |

## CHANGELOG

### Durable.DuckDb (new package)

- DuckDB provider on DuckDB.NET.Data.Full 1.5.6: `DuckDbDialect`, `DuckDbDataTypeConverter`, `DuckDbConnectionFactory`, `DuckDbRepositorySettings` (`ForInMemory`, `ForSharedInMemory`, `ForFile`, `Parse`, `IsInMemory`, `AccessMode`, `Threads`, `MemoryLimit`) and `DuckDbRepository<T>`.
- In-process database lifetime: the connection factory keeps a root connection open, so a `:memory:` database is shared by every repository on the factory and released with it; file and `:memory:?cache=shared` databases stay open for the factory's lifetime.
- Native types: unsigned integers (`UTINYINT` ... `UBIGINT`), `UUID`, `TIMESTAMP`/`TIMESTAMPTZ`, `DATE`, `TIME`, `INTERVAL`, `DECIMAL(38,10)`, `BLOB`, `JSON`, and `HUGEINT` for `BigInteger` provider types; BLOB streams and HUGEINT values are converted on read.
- Auto-increment keys through per-table sequences (`DEFAULT nextval`) with `INSERT ... RETURNING`; upsert through `INSERT ... ON CONFLICT`.
- `BulkInsert` through the DuckDB Appender (keys drawn from the sequence), with a prepared-INSERT fallback when column types differ from the mapping.
- Schema introspection through `information_schema` and `duckdb_columns()` / `duckdb_constraints()` / `duckdb_indexes()`; migration lock as a row in `durable_migration_lock` (stale rows from exited processes are replaced).
- Documented limitations: no savepoints, no stored procedures, string lengths not stored, migrations not wrapped in a transaction, optimistic write conflicts.

### Durable.Sql

- New `ISqlDialect` members, all with defaults on `SqlDialect` that keep existing behavior (custom dialects implementing `ISqlDialect` directly must add them): `SupportsSavepoints`, `DriverEnforcesCommandTimeout`, `SupportsStringMaxLength`, `AlterTableRequiresDroppingIndexes`, `IsMigrationLockContention(Exception)`.
- `ISqlTransaction.CreateSavepoint(Async)` throws `NotSupportedException` when the dialect does not support savepoints.
- The command executor enforces `SqlRepositoryOptions.CommandTimeoutSeconds` (cancel, then `TimeoutException`) for drivers that ignore `DbCommand.CommandTimeout`.
- The migrator treats a lock attempt that fails with a dialect-reported contention error as "not acquired" and polls again.
- Schema comparison wraps column drops and NOT NULL column additions with index drop/re-create for dialects that require it.
- `Sum`/`Average` results are converted through the data type converter (drivers may return aggregates as types without `IConvertible`, such as `BigInteger`).
- `RepositoryType.DuckDb`.

### Durable.Sqlite

- `SqliteDialect.SupportsStringMaxLength` is false (SQLite never stored declared lengths; this only reports it).

### Durable.Tool

- `--provider duckdb`: migrations, rollback, status, script, `migrations add`, schema diff/sync and scaffolding on DuckDB (DuckDB types scaffold to `bool`, `sbyte`/`byte`, `short`/`ushort`, `int`/`uint`, `long`/`ulong`, `float`, `double`, `decimal`, `DateTime`, `DateTimeOffset`, `DateOnly`, `TimeOnly`, `TimeSpan`, `Guid`, `byte[]`, `string`).

### Tests and CI

- DuckDB runs every SQL suite (721 cases incl. the new `DuckDbProvider` suite) in process, in memory or on a file (`--type duckdb [--filename <path>]`); CI job "DuckDB" on Linux, Windows and macOS for net8.0 and net10.0.
- Suites gate on dialect capabilities instead of database names where DuckDB differs: savepoints (`SupportsSavepoints`, asserting `NotSupportedException`), stored procedures (`SupportsStoredProcedures`), string lengths (`SupportsStringMaxLength`); raw ADO.NET helpers use the dialect's parameter names.
- `Test.Aot` runs a DuckDB scenario on .NET 9+ (DuckDB.NET.Data compiled in single-warn mode, like LiteDB); its ledger migration uses portable SQL.

## CLAUDE.md

Project tree (after `Durable.Sqlite/`):

```
├── Durable.DuckDb/            # DuckDB implementation (embedded, in-process; DuckDB.NET.Data.Full)
```

Database-Specific Implementations: "Each database provider (Sqlite, DuckDb, MySql, Postgres, SqlServer, ...) contains only: ..."

Build commands: `dotnet build src/Durable.DuckDb/Durable.DuckDb.csproj`

Running tests, after the SQLite line:

```bash
# DuckDB, in process (no docker); --filename <path> for a database file
dotnet run --project src/Test.Automated/Test.Automated.csproj -f net8.0 -- --type duckdb
```

Published packages list: add `Durable.DuckDb` (after `Durable.Sqlite`) and update the count.

Notes (Important Implementation Notes or Common Patterns):

- **DuckDB** (`Durable.DuckDb`) is in-process. `DuckDbConnectionFactory` keeps a root connection open for its lifetime (DuckDB has no pool; a database lives while a connection is open): `:memory:` is private to one factory (connections are `Duplicate()`s of the root), `:memory:?cache=shared` is process-wide, files hold DuckDB's single-writer-process lock until the factory is disposed. Concurrency is optimistic MVCC ("Conflict on update" instead of waiting). Dialect flags: `SupportsSavepoints`, `SupportsStoredProcedures`, `SupportsTransactionalDdl`, `SupportsStringMaxLength` false; `AlterTableRequiresDroppingIndexes` and `!DriverEnforcesCommandTimeout` true. Parameters are `$p0` (the dialect strips `$` for DuckDB.NET). Auto-increment keys are sequences `{table}_{column}_seq`; the migration lock is a row in `durable_migration_lock`. Gate tests on these dialect flags, never on `TestDatabaseType.DuckDb`.
- Dependencies: Durable.DuckDb uses DuckDB.NET.Data.Full 1.5.6 (not trim-annotated; Test.Aot compiles it single-warn on .NET 9+, like LiteDB).

## Limitations and capability gates (summary)

| Gate | Value on DuckDB | Why | README wording |
|---|---|---|---|
| `ISqlDialect.SupportsSavepoints` | false | DuckDB 1.5 has no `SAVEPOINT` | "Savepoints are not supported ... throws `NotSupportedException`" |
| `SupportsStoredProcedures` | false | DuckDB has macros, not procedures | "Stored procedures: not supported" |
| `SupportsTransactionalDdl` | false | Index DDL cannot share a transaction with row changes / index drops on the same table | "Migrations run statement by statement" |
| `SupportsStringMaxLength` | false | `VARCHAR(n)` lengths are not stored | "String lengths" |
| `AlterTableRequiresDroppingIndexes` | true | `DROP COLUMN`, `SET NOT NULL`, type changes fail while indexes exist | "Altering indexed tables" |
| `DriverEnforcesCommandTimeout` | false | DuckDB.NET ignores `CommandTimeout` | "Command timeout" |
| Native AOT on .NET 8 | not verified in Test.Aot | .NET 8 ILC cannot summarize DuckDB.NET's warnings | Native AOT provider-support row |
